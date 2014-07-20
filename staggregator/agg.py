#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__version__ = '0.1'
__author__ = 'Rob Tandy'
__license__ = 'MIT'

from asyncio import async, coroutine, open_connection, get_event_loop, sleep, \
                wait_for, TimeoutError, start_server
import argparse
import logging
import sys
import re
import time
import pickle
import struct
from statistics import mean, median, stdev

log = logging.getLogger('staggregator')

class Agg:
    def __init__(self, my_host, my_port, carbon_host, carbon_port, 
            flush_interval, percent_thresholds, connection_timeout=10,
            top_namespace='stats'):
        self.my_host = my_host 
        self.my_port = my_port 
        self.carbon_host = carbon_host
        self.carbon_port = carbon_port
        self.flush_interval = flush_interval
        self.metrics = {}
        self.percent_thresholds = percent_thresholds
        self.connection_timeout = connection_timeout
        self.ns_prefix = top_namespace

    def serv(self):
        loop = get_event_loop()
        s = start_server(self.client_connected, self.my_host, self.my_port,
                loop=loop)
        try:
            log.info('starting event loop')
            log.info('listening on {0}:{1}'.format(self.my_host, self.my_port))
            loop.call_soon(self.flusher)
            loop.run_until_complete(s)
            loop.run_forever()
        except KeyboardInterrupt as k:
            log.info('ending loop')
        finally:
            loop.close()
        log.info('done serving')

    def client_connected(self, client_reader, client_writer):
        c = ConnectedClient(client_reader, client_writer, self)
        async(c.handle())
    
    def flusher(self):
        now = time.time()
        messages = []

        for metric in self.metrics.values():
            if (now - metric.last_flush_time) > self.flush_interval:
                messages += metric.flush(now)
        if len(messages) > 0:
            log.debug('async send messages')
            async(self.send_messages(messages))

        get_event_loop().call_later(0.25, self.flusher)

    @coroutine
    def send_messages(self, messages):
        log.info('sending message {}'.format(messages))

        # use pickle protocol = 2, so python2 can read it
        payload = pickle.dumps(messages, 2)
        header = struct.pack("!L", len(payload))
        message = header + payload

        try:
            reader, writer = yield from wait_for(
                    open_connection(self.carbon_host, self.carbon_port),
                    self.connection_timeout)
            #FIXME, how to ensure messages are sent
            writer.write(message)
            writer.write_eof()
        except Exception as e:
            log.warning('Could not connect to carbon {}'.format(e))
            log.exception(e)
            # FIXME dump message to a log


class Metric:
    def __init__(self, name, ns_prefix, flush_interval):
        self.name = name
        self.accumulated_value = 0.0
        self.flush_interval = flush_interval
        self.ns_prefix = ns_prefix
        self.last_flush_time = time.time()
        log.info('metric {} created'.format(name))

    def accumulate(self, value):
        pass

    def flush(self, now):
        log.debug('flushing {}'.format(self.name))
        m = self.get_messages(now)
        
        self.last_flush_time = now
        self.reset()
        return m

    def reset(self):
        pass

    def get_messages(self): return []

class CountMetric(Metric):
    def accumulate(self, value):
        self.accumulated_value += value
        self.rate = self.accumulated_value / self.flush_interval

    def get_messages(self, now):
        z = self.ns_prefix + '.counters.'

        return [(z+self.name + '.count', (now, self.accumulated_value)),
                (z+self.name + '.rate', 
                    (now, self.accumulated_value / self.flush_interval))]

    def reset(self):
        self.accumulated_value = 0.0

class TimerMetric(Metric):
    def __init__(self, name, ns_prefix, flush_interval, percent_thresholds):
        super().__init__(name, ns_prefix, flush_interval)
        self.values = []
        self.thresholds = percent_thresholds

    def accumulate(self, value):
        self.values.append(value)

    def _nth_percentile(self, sorted_dataset, percentile):
        new_length = max(int(len(sorted_dataset) * percentile / 100), 1)
        return sorted_dataset[0:new_length]
         
    def get_messages(self, now):
        # using snippit here: https://gist.github.com/ageron/5212412
        messages = []
        z = self.ns_prefix + '.timers.'
        sorted_values = sorted(self.values)

        for percentile in self.thresholds:
            p = self._nth_percentile(sorted_values, percentile)
            s = '_' + str(percentile)
            
            messages.append((z+self.name + '.count'+s, (now, len(p))))
            messages.append((z+self.name + '.mean'+s, (now, mean(p))))
            messages.append((z+self.name + '.upper'+s, (now, p[-1])))
            messages.append((z+self.name + '.sum'+s, (now, sum(p))))

        messages.append((z+self.name + '.count', (now, len(sorted_values))))
        messages.append((z+self.name + '.count_ps', 
            (now, len(sorted_values) / self.flush_interval)))
        messages.append((z+self.name + '.mean', (now, mean(sorted_values))))
        messages.append((z+self.name + '.median', (now, median(sorted_values))))
        messages.append((z+self.name + '.std', (now, stdev(sorted_values))))
        messages.append((z+self.name + '.upper', (now, sorted_values[-1])))
        messages.append((z+self.name + '.lower', (now, sorted_values[0])))
        messages.append((z+self.name + '.sum', (now, sum(sorted_values))))

        return messages

    def reset(self):
        self.values = []


class ConnectedClient:
    def __init__(self, reader, writer, agg):
        self.reader = reader
        self.writer = writer
        self.line_re = re.compile('^([\w\.]+):(\w+)\|(\w+)$')
        self.agg = agg

    @coroutine
    def _nextline(self):
        return (yield from self.reader.readline()).decode('utf-8').rstrip()
    
    @coroutine
    def handle(self):

        while True:
            line = yield from self._nextline()
            if not line:
                break
            # lines are in the format <metricname>:<value>|<type>, per statsd
            match = self.line_re.match(line)
            if match is None:
                log.debug('bad line received {}'.format(line))
                continue
            metric_name, value, metric_type = match.groups()
            value = float(value)
            self.handle_stat(metric_name, value, metric_type)


    def handle_stat(self, metric_name, value, metric_type):
        if metric_type == 'c':
            if not metric_name in self.agg.metrics:
                self.agg.metrics[metric_name] = CountMetric(metric_name,
                        self.agg.ns_prefix, self.agg.flush_interval)
        elif metric_type == 'ms':
            if not metric_name in self.agg.metrics:
                self.agg.metrics[metric_name] = TimerMetric(metric_name,
                        self.agg.ns_prefix, self.agg.flush_interval,
                        self.agg.percent_thresholds)

        self.agg.metrics[metric_name].accumulate(value)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.ERROR,
            format='%(asctime)s | %(levelname)s | %(message)s')

    p = argparse.ArgumentParser()
    p.add_argument('-l', '--log-level', default='INFO')
    p.add_argument('-H', '--host', type=str, 
            help='hostname for listening socket', default='127.0.0.1')
    p.add_argument('-p', '--port', type=int,
            help='listening port for stats', default=5201)
    p.add_argument('-c', '--carbon-host', type=str, 
            help='hostname of carbon-cache, carbon-relay, \
                    or carbon-aggregator')
    p.add_argument('-q', '--carbon-port', type=int,
            help='carbon pickle port number', default=2004)

    p.add_argument('-f', '--flush-interval', type=int, default=10,
            help='flush interval (seconds)')
    p.add_argument('-t', '--percent-thresholds', type=str, default='90',
            help='comma separated percent threshold list')
    
    args = p.parse_args()
    log.setLevel(getattr(logging, args.log_level, logging.INFO))
    
    log.debug('got args {}'.format(args))

    pts = [int(x) for x in args.percent_thresholds.split(',')]

    Agg(args.host, args.port, args.carbon_host, args.carbon_port,
            args.flush_interval, pts).serv()
