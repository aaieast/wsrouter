import { DurableObject } from "cloudflare:workers";

// Worker
export default { async fetch(request, env, ctx) {
	// if (!request.url.endsWith("/websocket")) return new Response(`Supported endpoints: /websocket: Expects a WebSocket upgrade request`, {status: 200, headers: {"Content-Type": "text/plain"}});
    
	const upgradeHeader = request.headers.get("Upgrade");
	if (upgradeHeader && upgradeHeader == "websocket") {
		if (request.method !== "GET") return new Response("Worker expected GET method", {status: 400});
		
		// Since we are hard coding the Durable Object ID by providing the constant name 'foo', all requests to this Worker will be sent to the same Durable Object instance.
		return env.WEBSOCKET_HIBERNATION_SERVER.getByName("foo").fetch(request);
	}
    
    // return new Response("Worker expected Upgrade: websocket", {status: 426});
	return new Response(`<script>
var codeClient = new WebSocket("wss://" + window.location.toString().slice(8, -1));
codeClient.onopen = function() { 
	let server = parseInt(new URLSearchParams(window.location.search).get("server")) || 1;
	codeClient.send(JSON.stringify({server})); 
}
codeClient.onmessage = function(event) {
	let code = JSON.parse(event.data).code;
	if (code !== undefined) document.body.appendChild(document.createRange().createContextualFragment(code));
	else document.body.appendChild(document.createRange().createContextualFragment("No code delivered."));
};
<\/script>`, {status: 200, headers: {"Content-Type": "text/html"}});
}};

// Durable Object
export class WebSocketHibernationServer extends DurableObject {
	sessions; // Keeps track of all WebSocket connections. When the DO hibernates, gets reconstructed in the constructor
	clients;
	servers;
	constructed;
  
	constructor(ctx, env) {
		super(ctx, env);
		this.clients = [];
		this.servers = [];
		
		this.sessions = new Map(); // As part of constructing the Durable Object, we wake up any hibernating WebSockets and place them back in the `sessions` map.
		// Get all WebSocket connections from the DO. If we previously attached state to our WebSocket, let's add it to `sessions` map to restore the state of the connection.
		this.ctx.getWebSockets().forEach((ws) => { 
			let attachment = ws.deserializeAttachment();
			this.clients[attachment.client] = ws;
			if (attachment.server) this.servers[attachment.server] = ws;
			if (attachment) this.sessions.set(ws, { ...attachment });
		});

		this.constructed = true;
    	this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong")); // Sets an application level auto response that does not wake hibernated WebSockets.
	}

	async fetch(request) {
		const [client, server] = Object.values(new WebSocketPair());  // Creates two ends of a WebSocket connection.
		this.ctx.acceptWebSocket(server);
		
		// Attach the session ID to the WebSocket connection and serialize it. This is necessary to restore the state of the connection when the Durable Object wakes up.
		server.serializeAttachment({ client:this.clients.length });
		this.sessions.set(server, { client:this.clients.length }); // Add the WebSocket connection to the map of active sessions.
		this.clients.push(server);
		
		return new Response(null, {status: 101, webSocket: client});
	}

	async webSocketMessage(ws, data) {
		// Get the session associated with the WebSocket connection.
		const session = this.sessions.get(ws);
		let prefix = `[Durable Object] message: ${data}, from: ${session.client}, to: `;
		let suffix = `. Total connections: ${this.sessions.size}`;
		
		// Upon receiving a message from the client, the server echos the message, the session ID of the connection, and the total number of connections
		ws.send(prefix + `the initiating client` + suffix);
		this.sessions.forEach((attachment, connectedWs) => { connectedWs.send(prefix + `all clients` + suffix); });
		this.sessions.forEach((attachment, connectedWs) => { if (connectedWs !== ws) connectedWs.send(prefix + `all clients except the initiating client` + suffix); });
		
		try { data = JSON.parse(data); }
		catch { return false; }

		if (session.server !== undefined) { 
			let client = this.clients[data.client];    
			if (!session.server) {
				if (data.server) {
					client.serializeAttachment({client:data.client, server:data.server});
					this.sessions.set(client, {client:data.client, server:data.server});
					this.servers[data.server] = client;
					data = {msg:0}; // Server ID confirmed.
				} else data = {msg:2}; // Invalid key.
			}
			delete data.client;
			if (client) client.send(JSON.stringify(data));
		} else if (this.servers[data.server]) {
			data.client = session.client;
			let server = this.servers[data.server];
			delete data.server;
			server.send(JSON.stringify(data));
		} else if (data.key !== undefined) {
			data.client = session.client;
			if (this.servers[0]) this.servers[0].send(JSON.stringify(data));
			else if (data.key == "secret") {
				let noServer = !this.servers[data.server];
				ws.serializeAttachment({client:data.client, server:data.server});
				this.sessions.set(ws, {client:data.client, server:data.server});
				this.servers[data.server] = ws;
				let noServerAfter = !this.servers[data.server];
				ws.send(JSON.stringify({msg:0, theServers:this.servers, data, noServer, noServerAfter, constructed:this.constructed})) // Server ID confirmed.
			}
			else ws.send(JSON.stringify({msg:2})); // Invalid key.
		} else ws.send(JSON.stringify({msg:1})); // Server offline.
	}

	async webSocketClose(ws, code, reason, wasClean) {
		delete this.servers[this.sessions.get(ws).server];
		this.sessions.delete(ws);
		ws.close(code, "Durable Object is closing WebSocket");
	}
}
