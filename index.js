import { DurableObject } from "cloudflare:workers";

// Worker
export default {
  async fetch(request, env, ctx) {
    if (!request.url.endsWith("/websocket")) return new Response(`Supported endpoints: /websocket: Expects a WebSocket upgrade request`, {status: 200, headers: {"Content-Type": "text/plain"}});
    
    // Expect to receive a WebSocket Upgrade request. If there is one, accept the request and return a WebSocket Response.
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader !== "websocket") return new Response("Worker expected Upgrade: websocket", {status: 426});
    if (request.method !== "GET") return new Response("Worker expected GET method", {status: 400});

    // Since we are hard coding the Durable Object ID by providing the constant name 'foo', all requests to this Worker will be sent to the same Durable Object instance.
    return env.WEBSOCKET_HIBERNATION_SERVER.getByName("foo").fetch(request);
  },
};

// Durable Object
export class WebSocketHibernationServer extends DurableObject {
  sessions; // Keeps track of all WebSocket connections. When the DO hibernates, gets reconstructed in the constructor
  clients;
  
  constructor(ctx, env) {
    super(ctx, env);
    this.sessions = new Map(); // As part of constructing the Durable Object, we wake up any hibernating WebSockets and place them back in the `sessions` map.
    this.clients = [];
    
    // Get all WebSocket connections from the DO. If we previously attached state to our WebSocket, let's add it to `sessions` map to restore the state of the connection.
    this.ctx.getWebSockets().forEach((ws) => { 
      let attachment = ws.deserializeAttachment();
      if (attachment) this.sessions.set(ws, { ...attachment });
      this.clients[attachment.id] = ws;
    });

    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong")); // Sets an application level auto response that does not wake hibernated WebSockets.
  }

  async fetch(request) {
    const [client, server] = Object.values(new WebSocketPair());  // Creates two ends of a WebSocket connection.
    this.ctx.acceptWebSocket(server);

    // Attach the session ID to the WebSocket connection and serialize it. This is necessary to restore the state of the connection when the Durable Object wakes up.
    const id = this.clients.length;
    this.clients.push(server);
    server.serializeAttachment({ id });
    this.sessions.set(server, { id }); // Add the WebSocket connection to the map of active sessions.

    return new Response(null, {status: 101, webSocket: client});
  }

  async webSocketMessage(ws, message) {
    // Get the session associated with the WebSocket connection.
    const session = this.sessions.get(ws);
    let prefix = `[Durable Object] message: ${message}, from: ${session.id}, to: `;
    let suffix = `. Total connections: ${this.sessions.size}`;

    // Upon receiving a message from the client, the server echos the message, the session ID of the connection, and the total number of connections
    ws.send(prefix + `the initiating client` + suffix);
    this.sessions.forEach((attachment, connectedWs) => { connectedWs.send(prefix + `all clients` + suffix); });
    this.sessions.forEach((attachment, connectedWs) => { if (connectedWs !== ws) connectedWs.send(prefix + `all clients except the initiating client` + suffix); });
  }

  async webSocketClose(ws, code, reason, wasClean) {
    this.sessions.delete(ws);
    ws.close(code, "Durable Object is closing WebSocket");
  }
}
