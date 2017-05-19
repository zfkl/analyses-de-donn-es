"""
Allows to handle websockets,
maintains clients list
send json to web browser
"""

import tornado.ioloop
import tornado.web
import tornado.websocket

from tornado.options import define, options, parse_command_line

define("port", default=443, help="run on the given port", type=int)

# we gonna store clients in dictionary..
clients = set()

class IndexHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        #self.write("This is your response")
        self.render("visu.html")
        print "MAIN HANDLER"
        print self.request



class WebSocketHandler(tornado.websocket.WebSocketHandler):
    def open(self, *args):
        print"SOCKET HANDLER"
        print self.request
        clients.add(self)
        print" clients size is {}".format(len(clients))



    def on_message(self, message):
        """
        when we receive some message we send json to visu client
        """
        print "Client %s received a message : %s" % (self, message)
        print "envoyer ceci pour les dashboards dynamiques"
        for c in clients:
           c.write_message(message)
        #clients[self.id] = {"id": self.id, "object": self}
        print message

    def on_close(self):
        print "close"
        print" clients size is {}".format(len(clients))
        if self in clients:
            clients.remove(self)

app = tornado.web.Application([
    (r'/', IndexHandler),
    (r'/websocket', WebSocketHandler),
])

if __name__ == '__main__':
    parse_command_line()
    app.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
