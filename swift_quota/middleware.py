# -*- encoding: utf-8 -*-
from swift.common.utils import get_logger, cache_from_env, split_path
from swift.common.swob import Request, Response
from swift.common.http import HTTP_OK
from swift.proxy.controllers.base import get_account_memcache_key


# Move to swift.proxy.controllers.base
def headers_to_account_info(headers, status_int=HTTP_OK):
    """
    Construct a cacheable dict of account info based on response headers.
    """
    headers = dict(headers)
    return {
        'status': status_int,
        'container_count': headers.get('x-account-container-count'),
        'object_count': headers.get('x-account-object-count'),
        'bytes': headers.get('x-account-bytes-used'),
        'meta': dict((key.lower()[15:], value)
                     for key, value in headers.iteritems()
                     if key.lower().startswith('x-account-meta-'))
    }


def get_account_info(env, app, logger):
    """
    Get the info structure for an account, based on env and app.
    This is useful to middlewares.
    """
    #TODO: memcachet
    container_info = env.get('swift.container_info')
    if not container_info:
        cache = cache_from_env(env)
        if not cache:
            return None
        (version, account, _, _) = \
            split_path(env['PATH_INFO'], 2, 4, True)
        cache_key = get_account_memcache_key(account)
        account_info = cache.get(cache_key)
        if account_info:
            print "Using memcache."
        if not account_info:
            new_env = dict(env, REQUEST_METHOD='HEAD')
            resp = Request.blank('/%s/%s' % (version, account),
                                 environ=new_env).get_response(app)
            account_info = headers_to_account_info(resp.headers)
            #TODO: use recheck_account_existence for timeout.
            cache.set(cache_key, account_info, timeout=60)
    return account_info


class QuotaMiddleware(object):
    def __init__(self, app, conf):
        self.app = app
        self.conf = conf

        self.logger = get_logger(self.conf, log_route='quota')

    def __call__(self, env, start_response):
        req = Request(env)
        if not req.method == "PUT":
            return self.app(env, start_response)
        account_info = get_account_info(env, self.app, self.logger)
        print account_info
        if not account_info:
            return self.app(env, start_response)

        if 'quota' in account_info.get('meta', {}) and \
                'bytes' in account_info and \
                account_info['meta']['quota'].isdigit():
            new_size = int(account_info['bytes']) + \
                int(env.get('CONTENT_LENGTH', 0))
            self.logger.debug("Quota is %s" % (new_size))
            if int(account_info['meta']['quota']) < new_size:
                return Response(status=413, body='Upload exceeds quota.')(
                    env, start_response)

        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    return lambda app: QuotaMiddleware(app, conf)
