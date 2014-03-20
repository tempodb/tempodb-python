import protocol
import json


SUCCESS = 0
FAILURE = 1
PARTIAL = 2


class ResponseException(Exception):
    """Exception class for HTTP responses"""

    def __init__(self, response):
        self.response = response
        self.msg = 'TempoDB response returned status: %d' % response.status

    def __repr__(self):
        return self.msg

    def __str__(self):
        return self.msg


class Response(object):
    """Represents responses from the TempoDB API.  The Response object has
    several useful attributes after it is created:

        * successful: whether the overall request was successful (see below)
        * status: the HTTP status code for the API call
        * reason: the explanation for the HTTP status code
        * data: an object or list of objects representing the data from the API
        * error: a string if the API returned any additional error information
                 in the response body, None otherwise

    **Note:** data will be None if the status code was anything other than
    200.

    **Note:** successful has 3 possible values defined as constants in this
    module, SUCCESS, FAILURE, and PARTIAL.  A PARTIAL value can occur during a
    multi-write if some datapoints fail to write.  The error attribute in that
    case will be a JSON encoded string of errors for each datapoint.  The
    response object does *not* derserialize that error (there could be other
    circumstances where the error is not JSON encoded), so if that case, error
    handling code surrounding multi-writes should decode the error attribute
    with the json library if it wants to attempt error recovery.

    :param obj resp: a response object from the requests library"""

    def __init__(self, resp, session):
        self.resp = resp
        self.session = session
        self.status = resp.status_code
        self.reason = resp.reason
        if self.status == 200:
            self.successful = SUCCESS
            self.error = None
        elif self.status == 207:
            self.successful = PARTIAL
            self.error = self.resp.text
        else:
            self.successful = FAILURE
            self.error = self.resp.text

        self.resp.encoding = "UTF-8"
        self.body = self.resp.text
        self.data = None

    def _cast_payload(self, t):
        if type(t) == list:
            obj = getattr(protocol, t[0])
            l = json.loads(self.resp.text)
            self.data = [obj(d, self) for d in l]
        else:
            obj = getattr(protocol, t)
            self.data = obj(self.resp.text, self)
