#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Rob Tandy'
__license__ = 'MIT'

from asyncio import async, coroutine, open_connection, get_event_loop, sleep, \
                wait_for, TimeoutError, start_server
import argparse
import logging
import sys
import re
import os
import time
import json
import pickle
import struct
import configparser
from statistics import mean, median, stdev
from microhttpd import Application, HTTPException

log = logging.getLogger('staggregator')

class Agg:
    def __init__(self, my_host, my_port, carbon_host, carbon_port, 
            flush_interval, percent_thresholds, *, 
            keys=None, connection_timeout=10, top_namespace='stats',
            auth_header_name=None):
        self.my_host = my_host 
        self.my_port = my_port 
        self.carbon_host = carbon_host
        self.carbon_port = carbon_port
        self.flush_interval = flush_interval
        self.metrics = {}
        self.percent_thresholds = percent_thresholds
        self.connection_timeout = connection_timeout
        self.ns_prefix = top_namespace
        self.keys = {}
        self.auth_header_name = auth_header_name

        if keys:
            # auth header is required if keys are present so check that
            if not self.auth_header_name:
                raise Exception("auth_header_name cannot be None with keys \
                        provided")
            self.auth_header_name = self.auth_header_name.lower()

            for key, regex in keys:
                self.keys[key] = re.compile(regex)
        

        self.app = Application([
            (r'^/v1/stat/?$', 'POST', self.v1_stat),
            ])
        
    def serve(self):
        loop = get_event_loop()
        try:
            log.info('starting event loop')
            log.info('listening on {0}:{1}'.format(self.my_host, self.my_port))
            log.info('sending data to carbon-cache at {0}:{1}'.format(
                self.carbon_host, self.carbon_port))
            log.info('flush interval to carbon is {0}s'.format(
                self.flush_interval))
            log.info('calculating {}th percentile(s)'.format(
                ','.join(map(str,self.percent_thresholds))))
            
            log.info('listening on {0}:{1}'.format(self.my_host, self.my_port))
            loop.call_soon(self.flusher)
            self.app.serve(self.my_host, self.my_port, keep_alive=False)
        except KeyboardInterrupt as k:
            log.info('Keyboard Interrupt.  Stopping server.')
        finally:
            loop.close()
        log.info('done serving')
    
    @coroutine
    def v1_stat(self, request, response):
        body = yield from request.body()
        metrics = json.loads(body.decode('utf-8'))
        log.debug('loaded {}'.format(metrics))

        # fixme, validate json with validictory

        for metric in metrics:
            # get namespace
            ns = metric['name'].split('.')[0]

            # check auth if we have keys
            if len(self.keys) > 0:
                key = request.headers.get(self.auth_header_name, None)
                if key is None or \
                        not key in self.keys or \
                        not self.keys[key].match(ns):
                    log.warning('{0} Unauthorized for {1}'.format(key, ns))
                    raise HTTPException(401, 'Unauthorized')

            # got here, then auth is ok
            self.handle_stat(metric['name'], metric['value'], metric['type'])
    
    def handle_stat(self, metric_name, value, metric_type):
        if metric_type == 'c':
            if not metric_name in self.metrics:
                self.metrics[metric_name] = CountMetric(metric_name,
                        self.ns_prefix, self.flush_interval)
        elif metric_type == 'ms':
            if not metric_name in self.metrics:
                self.metrics[metric_name] = TimerMetric(metric_name,
                        self.ns_prefix, self.flush_interval,
                        self.percent_thresholds)

        self.metrics[metric_name].accumulate(value)
    
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

    def get_messages(self, timestamp): return []


# count_ps_peak and rate_peak will be aggregated using the max 
# aggregation method vs avg (assumping carbon is set up properly for this).
# Now we can keep track of peak rate in this interval in addition
# to average rate

class CountMetric(Metric):
    def accumulate(self, value):
        self.accumulated_value += value
        self.rate = self.accumulated_value / self.flush_interval

    def get_messages(self, now):
        z = self.ns_prefix + '.counters.'

        return [(z+self.name + '.count', (now, self.accumulated_value)),
                (z+self.name + '.rate', 
                    (now, self.accumulated_value / self.flush_interval)),
                (z+self.name + '.rate_peak', 
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

            m = mean(p) if len(p) > 0 else 0
            u = p[-1] if len(p) > 0 else 0
            
            messages.append((z+self.name + '.count'+s, (now, len(p))))
            messages.append((z+self.name + '.mean'+s, (now, m)))
            messages.append((z+self.name + '.upper'+s, (now, u)))
            messages.append((z+self.name + '.sum'+s, (now, sum(p))))

        m = median(sorted_values) if len(sorted_values) > 0 else 0
        e = mean(sorted_values) if len(sorted_values) > 0 else 0
        u = sorted_values[-1] if len(sorted_values) > 0 else 0
        l = sorted_values[0] if len(sorted_values) > 0 else 0
        d = stdev(sorted_values) if len(sorted_values) > 1 else 0

        messages.append((z+self.name + '.count', (now, len(sorted_values))))
        messages.append((z+self.name + '.count_ps', 
            (now, len(sorted_values) / self.flush_interval)))
        messages.append((z+self.name + '.count_ps_peak', 
            (now, len(sorted_values) / self.flush_interval)))
        messages.append((z+self.name + '.median', (now, m)))
        messages.append((z+self.name + '.upper', (now, u)))
        messages.append((z+self.name + '.lower', (now, l)))
        messages.append((z+self.name + '.sum', (now, sum(sorted_values))))
        messages.append((z+self.name + '.mean', (now, e)))
        messages.append((z+self.name + '.std', (now, d)))

        return messages

    def reset(self):
        self.values = []

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.ERROR,
            format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')

    p = argparse.ArgumentParser()
    p.add_argument('-l', '--log-level', default='ERROR')
    p.add_argument('-H', '--host', type=str, 
            help='hostname for listening socket', default='127.0.0.1')
    p.add_argument('-p', '--port', type=str,
            help='listening port for stats', default='5201')
    p.add_argument('-c', '--carbon-host', type=str, default='127.0.0.1',
            help='hostname of carbon-cache, carbon-relay, \
                    or carbon-aggregator')
    p.add_argument('-q', '--carbon-port', type=str,
            help='carbon pickle port number', default='2004')

    p.add_argument('-i', '--flush-interval', type=str, default='10',
            help='flush interval (seconds)')
    p.add_argument('-t', '--percent-thresholds', type=str, default='90',
            help='comma separated percent threshold list')
    p.add_argument('-f', '--config', type=str, default=None,
            help='path to config file')
    
    args = p.parse_args()
    log.setLevel(getattr(logging, args.log_level, logging.INFO))
    
    log.debug('got args {}'.format(args))
    
    params = ['host', 'port', 'carbon_host', 'carbon_port', 'flush_interval',
            'percent_thresholds']
    agg_vars = {}
    keys = []
    auth_header_name = None
    # get values from cmd line
    for param in params:
        value = getattr(args, param)
        agg_vars[param] = value
    
    # over ride with any from config if specified
    if args.config:
        # dummy section header to satisfy config parser
        s = open(args.config).read()
        c = configparser.ConfigParser()
        c.read_string(s)

        for param in params:
            v = c.get('staggregator', param, fallback=None)
            if v:
                agg_vars[param] = v

        # set up logging levels per config file
        for name, level in c.items('logging'):
            logging.getLogger(name).setLevel(getattr(logging, level))

        # get keys
        for key, regex in c.items('keys'):
            keys.append((key,regex))

        auth_header_name = c.get('staggregator', 'auth_header_name', 
                fallback=None)


    agg_vars['pts']=[int(x) for x in agg_vars['percent_thresholds'].split(',')]

    log.debug('using agg_vars {0}'.format(agg_vars))

    Agg(agg_vars['host'], int(agg_vars['port']), agg_vars['carbon_host'], 
            int(agg_vars['carbon_port']), int(agg_vars['flush_interval']),
            agg_vars['pts'], keys=keys, 
            auth_header_name=auth_header_name).serve()
