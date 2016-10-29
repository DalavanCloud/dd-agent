# stdlib
import re

# 3rd party
import memcache

# project
from checks import AgentCheck


# Based on the mcache check
class Kestrel(AgentCheck):

    SOURCE_TYPE_NAME = 'kestrel'

    DEFAULT_PORT = 22134

    # The stats I'm building this based on:
    # ross@sweetums$ echo stats | nc 127.0.0.1 22134
    # STAT uptime 65739197
    # STAT time 1477418214
    # STAT version 2.4.1
    # STAT curr_items 0
    # STAT total_items 1434411365
    # STAT bytes 0
    # STAT reserved_memory_ratio 0.032
    # STAT curr_connections 576
    # STAT total_connections 8059980
    # STAT cmd_get 49813725326
    # STAT cmd_set 1434411365
    # STAT cmd_peek 0
    # STAT get_hits 1434411857
    # STAT get_misses 48379278002
    # STAT bytes_read 5441530959174
    # STAT bytes_written 4192675779171
    # STAT queue_creates 1
    # STAT queue_deletes 0
    # STAT queue_expires 0
    # STAT queue_somequeue_items 0
    # STAT queue_somequeue_bytes 0
    # STAT queue_somequeue_total_items 1434411365
    # STAT queue_somequeue_logsize 762998
    # STAT queue_somequeue_expired_items 0
    # STAT queue_somequeue_mem_items 0
    # STAT queue_somequeue_mem_bytes 0
    # STAT queue_somequeue_age 0
    # STAT queue_somequeue_discarded 0
    # STAT queue_somequeue_waiters 101
    # STAT queue_somequeue_open_transactions 1
    # STAT queue_somequeue_transactions 1434411857
    # STAT queue_somequeue_canceled_transactions 492
    # STAT queue_somequeue_total_flushes 0
    # STAT queue_somequeue+fanned_items 0
    # STAT queue_somequeue+fanned_bytes 0
    # STAT queue_somequeue+fanned_total_items 11908616294
    # STAT queue_somequeue+fanned_logsize 6162831
    # STAT queue_somequeue+fanned_expired_items 0
    # STAT queue_somequeue+fanned_mem_items 0
    # STAT queue_somequeue+fanned_mem_bytes 0
    # STAT queue_somequeue+fanned_age 0
    # STAT queue_somequeue+fanned_discarded 0
    # STAT queue_somequeue+fanned_waiters 0
    # STAT queue_somequeue+fanned_open_transactions 0
    # STAT queue_somequeue+fanned_transactions 11920365096
    # STAT queue_somequeue+fanned_canceled_transactions 11748802
    # STAT queue_somequeue+fanned_total_flushes 0

    GAUGES = [
        "total_items",
        "curr_items",
        "uptime",
        "bytes",
        "reserved_memory_ratio",
        "curr_connections",
    ]

    RATES = [
        "cmd_get",
        "cmd_set",
        "cmd_peek",
        "get_hits",
        "get_misses",
        "bytes_read",
        "bytes_written",
        "queue_creates",
        "queue_deletes",
        "queue_expires",
        "total_connections",
    ]

    QUEUES_RATES = [
        "total_items",
        "expired_items",
        "discarded",
        "transactions",
        "canceled_transactions",
        "total_flushes",
    ]

    QUEUES_GAUGES = [
        "items",
        "bytes",
        "mem_items",
        "mem_bytes",
        "age",
        "waiters",
        "open_transactions",
    ]

    SERVICE_CHECK = 'kestrel.can_connect'

    # https://github.com/twitter-archive/kestrel/blob/master/docs/
    #     guide.md#a-working-guide-to-kestrel
    # Basically they say alphanumeric, -, and _, but really any char except: /,
    # ~, +, and . is legal in a name. + is apparently used for fanout queues.
    # This rules out regexes for all practical purposes and leaves us with
    # what's below, but there are even potential issues with it that they don't
    # document...

    # We need to order this from longest to shortest so that we match
    # most-specific first. That's because there's some overlap in the names,
    # e.g. mem_items & items. Note that can fail us if a queue name happens to
    # end in _mem. In reality that's just a problem with the "schema" kestrel is
    # using for things and there's nothing we can do about it.
    QUEUE_METRICS = sorted(QUEUES_RATES + QUEUES_GAUGES, key=lambda m: len(m),
                           reverse=True)

    def parse_queue_metric(self, metric):
        for queue_metric in self.QUEUE_METRICS:
            if metric.endswith(queue_metric):
                # 6 to skip queue_, n - 1 to omit the metric name, rest will be
                # the queue, we can ignore special chars as they'll be handled
                # for us
                return metric[6:-len(queue_metric) - 1], queue_metric
        # This doesn't look like a queue metric
        return None, None

    def _get_metrics(self, client, tags, service_check_tags=None):
        raw_stats = client.get_stats()

        assert len(raw_stats) == 1 and len(raw_stats[0]) == 2,\
            "Malformed response: %s" % raw_stats

        stats = raw_stats[0][1]
        for metric in stats:
            # Check if metric is a gauge or rate
            if metric in self.GAUGES:
                our_metric = self.normalize(metric.lower(), 'kestrel')
                self.gauge(our_metric, float(stats[metric]), tags=tags)
                continue

            # Tweak the name if it's a rate so that we don't use the exact
            # same metric name as the kestrel documentation
            if metric in self.RATES:
                our_metric = self.normalize(
                    "{0}_rate".format(metric.lower()), 'kestrel')
                self.rate(our_metric, float(stats[metric]), tags=tags)
                continue

            # See if it's a queue metric, we're prefixing these metrics with
            # `queue_` so that they won't collide with their global
            # counterparts.
            queue, queue_metric = self.parse_queue_metric(metric)
            if queue is not None:
                if queue_metric in self.QUEUES_GAUGES:
                    our_metric = self.normalize(
                        'queue_{0}'.format(queue_metric.lower()),
                        'kestrel')
                    self.gauge(our_metric, float(stats[metric]),
                               tags=tags + ['queue:{}'.format(queue)])
                    continue

                if queue_metric in self.QUEUES_RATES:
                    our_metric = self.normalize(
                        "queue_{0}_rate".format(queue_metric.lower()),
                        'kestrel')
                    self.rate(our_metric, float(stats[metric]),
                              tags=tags + ['queue:{}'.format(queue)])
                    continue

        uptime = stats.get("uptime", 0)
        self.service_check(
            self.SERVICE_CHECK, AgentCheck.OK,
            tags=service_check_tags,
            message="Server has been up for %s seconds" % uptime)

    def check(self, instance):
        socket = instance.get('socket')
        server = instance.get('host')

        if not server and not socket:
            raise Exception('Either "host" or "socket" must be configured')

        if socket:
            server = 'unix'
            port = socket
        else:
            port = int(instance.get('port', self.DEFAULT_PORT))
        custom_tags = instance.get('tags') or []

        client = None  # client
        tags = ["host:{0}:{1}".format(server, port)] + custom_tags
        service_check_tags = ["host:%s" % server, "port:%s" % port]

        try:
            self.log.debug("Connecting to %s:%s tags:%s", server, port, tags)
            client = memcache.Client(["%s:%s" % (server, port)])

            self._get_metrics(client, tags, service_check_tags)
        except AssertionError:
            self.service_check(
                self.SERVICE_CHECK, AgentCheck.CRITICAL,
                tags=service_check_tags,
                message="Unable to fetch stats from server")
            raise Exception(
                "Unable to retrieve stats from kestrel instance: {0}:{1}."
                "Please check your configuration".format(server, port))

        if client is not None:
            client.disconnect_all()
            self.log.debug("Disconnected from kestrel")
        del client
