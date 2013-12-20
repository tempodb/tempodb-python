import json


#UTILITY FUNCTIONS FOR MISCELLANEOUS PARTS OF API PROTOCOL
def make_series_key(key, tags, attributes):
    """Utility function for making a series key POST body, used mainly for
    the series creation API endpoint.

    :param string key: the series key
    :rtype: string"""

    return json.dumps({'key': key, 'tags': tags, 'attributes': attributes})
