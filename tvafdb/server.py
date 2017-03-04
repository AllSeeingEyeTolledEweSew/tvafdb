import BaseHTTPServer
import codecs
import json
import traceback
import urllib
import urlparse


class Handler(BaseHTTPServer.BaseHTTPRequestHandler):

    def __init__(self, tvafdb, *args):
        self.tvafdb = tvafdb
        self.writer = codecs.getwriter("utf-8")
        self._strwfile = None
        super(Handler, self).__init__(*args)

    @property
    def strwfile(self):
        if self._strwfile is None:
            self._strwfile = self.writer(self.wfile)
        return self._strwfile

    def do_GET(self):
        self.sent_headers = False

        try:
            url = urlparse.urlparse(self.path)
            path = urllib.unquote_plus(url.path)
            qd = urlparse.parse_qs(url.query)
            if path == "/metadata/timestamp":
                self.do_timestamp(**qd)
            elif path == "/metadata/feed":
                self.do_feed(**qd)
            elif path == "/metadata/search":
                self.do_search(urlparse.parse_qsl(url.query))
            elif path.startswith("/metadata/"):
                self.do_get(path[len("/metadata"):], **qd)
            else:
                self.send_response(404)
                self.end_headers()
                self.sent_headers = True
        except:
            self.handle_exc()
            return

    def handle_exc(self):
        if not self.sent_headers:
            self.send_response(500)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.sent_headers = True
        traceback.print_exc(None, self.strwfile)

    def do_timestamp(self):
        timestamp = self.tvafdb.get_timestamp()
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.sent_headers = True
        json.dump({"data": timestamp}, self.strwfile, indent=4, sort_keys=True)

    def do_feed(self, timestamp=None, keys=None):
        timestamp = timestamp[0] if timestamp else None
        feed = self.tvafdb.feed(timestamp=timestamp, keys=keys)
        self.send_response(200)
        self.send_header("Content-type", "application/x-json-stream")
        self.end_headers()
        self.sent_headers = True
        for entry in feed:
            entry = dict(
                action=entry.action, path=entry.path, updated=entry.updated,
                keys=sorted(entry.keys))
            json.dump(entry, self.strwfile, sort_keys=True)
            self.strwfile.write("\n")

    def do_get(self, path, keys=None):
        data = self.tvafdb.get(path, keys=keys)
        children = sorted(self.tvafdb.browse(path))
        result = {"data": data, "children": children}
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.sent_headers = True
        json.dump(result, self.strwfile, indent=4, sort_keys=True)

    def do_search(self, terms):
        result = list(self.tvafdb.search(terms))
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.sent_headers = True
        json.dump({"data": result}, self.strwfile, indent=4, sort_keys=True)


def main():
    import SocketServer
    import sys
    import tvafdb

    class ThreadingServer(SocketServer.ThreadingMixIn,
            BaseHTTPServer.HTTPServer):
        pass

    db = tvafdb.TvafDb(sys.argv[1])

    def MakeHandler(*args):
        return Handler(db, *args)

    server = ThreadingServer(("", 51909), MakeHandler)
    server.serve_forever()
