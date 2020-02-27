import logging

def cus_logger(log_file_name):
    # create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler(log_file_name)
    fh.setLevel(logging.DEBUG)
    # create console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers

    extra_fields = {'thread_id': 'THREAD-ID:',
                    'function_name': 'FUNCTION:'}
    formatter = logging.Formatter('%(asctime)s'
                                  ' [%(levelname)s]'
                                  ' %(processName)s'
                                  ' %(process)s'
                                  ' (%(threadName)s)'
                                  ' %(thread_id)s'
                                  ' %(thread)d'
                                  ' %(function_name)s'
                                  ' %(funcName)s'
                                  ' %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger = logging.LoggerAdapter(logger, extra_fields)

    return logger
