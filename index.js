import { DurableObject } from "cloudflare:workers";

// Worker
export default { async fetch(request, env, ctx) {
	if (request.headers.get("Upgrade") == "websocket") {
		if (request.method !== "GET") return new Response("Worker expected GET method", {status: 400});
		
		// Since we hardcode the DO ID by providing the constant name 'foo', all requests to this Worker will be sent to the same DO instance.
		return env.WEBSOCKET_HIBERNATION_SERVER.getByName("foo").fetch(request);
	}
    
	return new Response(`Loading...
<script>
var codeClient = new WebSocket("wss://" + window.location.host);
codeClient.onopen = function() { 
	let path = window.location.pathname.split("/");
	let server = (path.length && parseInt(path[path.length - 1])) || 1;
	codeClient.send(JSON.stringify({server})); 
}
codeClient.onmessage = function(event) {
	document.body.textContent = "";
	let code = JSON.parse(event.data).code || "No code delivered.";
	document.body.appendChild(document.createRange().createContextualFragment(code));
};
<\/script>`, {status: 200, headers: {"Content-Type": "text/html"}});
}};

// Durable Object
export class WebSocketHibernationServer extends DurableObject {
	sessions; // Tracks all WebSocket connections. When the DO hibernates, gets reconstructed in the constructor
	clients;
	servers;
	dns;
  
	constructor(ctx, env) {
		super(ctx, env);
		this.clients = [];
		this.servers = [];
		this.dns = env.dns;
		
		this.sessions = new Map(); // As part of constructing the Durable Object, wake any hibernating WebSockets and place them back in `sessions`
		this.ctx.getWebSockets().forEach((ws) => { 
			let attachment = ws.deserializeAttachment();
			this.sessions.set(ws, { ...attachment });
			this.clients[attachment.client] = ws;
			if (attachment.server !== undefined) this.servers[attachment.server] = ws;
		});

		// Sets an application level auto response that does not wake hibernated WebSockets
    	this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
	}

	async fetch(request) {
		const [client, server] = Object.values(new WebSocketPair()); 
		this.ctx.acceptWebSocket(server);
		
		// Attach session ID to the WebSocket connection and serialize it. This is needed to restore connection state when the DO wakes up.
		server.serializeAttachment({ client:this.clients.length });
		this.sessions.set(server, { client:this.clients.length }); // Add the WebSocket connection to the map of active sessions.
		this.clients.push(server);
		
		return new Response(null, {status: 101, webSocket: client});
	}

	async webSocketClose(ws, code, reason, wasClean) {
		delete this.servers[this.sessions.get(ws).server];
		this.sessions.delete(ws);
		ws.close(code, "Durable Object is closing WebSocket");
	}

	async webSocketMessage(ws, data) {
		const session = this.sessions.get(ws);
		
		// Upon receiving message from client, the server echos the message, the session ID of the connection, and the total number of connections
		let prefix = `[Durable Object] message: ${data}, from: ${session.client}, to: `;
		let suffix = `. Total connections: ${this.sessions.size}`;
		//ws.send(prefix + `the initiating client` + suffix);
		//this.sessions.forEach((attachment, connectedWs) => { connectedWs.send(prefix + `all clients` + suffix); });
		
		try { data = JSON.parse(data); }
		catch { return false; }
		
		function writeServer(ws, data, success, cls) {
			if (success) {
				ws.serializeAttachment({client:data.client, server:data.server});
				cls.sessions.set(ws, {client:data.client, server:data.server});
				cls.servers[data.server] = ws;
				ws.send(JSON.stringify({msg:0})); // Server ID confirmed.
			} else ws.send(JSON.stringify({msg:2})); // Invalid key.
		}
		
		if (session.server !== undefined) { 
			let client = this.clients[data.client];    
			if (!session.server && !data.key) return writeServer(client, data, data.server, this);
			delete data.client;
			if (client) client.send(JSON.stringify(data));
		} else if (this.servers[data.server]) {
			data.client = session.client;
			let server = this.servers[data.server];
			delete data.server;
			server.send(JSON.stringify(data));
		} else if (data.key) {
			data.client = session.client;
			if (!data.server) return writeServer(ws, data, data.key == this.dns, this);
			if (this.servers[0]) this.servers[0].send(JSON.stringify(data));
			else ws.send(JSON.stringify({msg:1})); // Server offline.
		} else ws.send(JSON.stringify({msg:1})); // Server offline.
	}
}
