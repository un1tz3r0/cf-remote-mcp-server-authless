import { McpAgent } from "agents/mcp";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { getEnv } from '@repo/mcp-common/src/env'
import { KVTreeStorage } from "./cfkvtree.js";

interface Env {
	globaldata: KVNamespace
};

const env = getEnv<Env>();

// Define our MCP agent with tools
export class MyMCP extends McpAgent {
	server = new McpServer({
		name: "Global Shared KV Storage",
		version: "1.0.0",
	});

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env)
	}
	
	async init() {
		// get a value at a given path in the global kv storage
		this.server.tool(
			"getValue",
			{ path: Array<String> },
			async ({ path }) => {
				const tree = KVTreeStorage(this.env['globaldata']);
				tree.initializeRoot();
				var result = null;
				if(tree.nodeExists(path)) {
					result = tree.getValue(path);
				}
				return ({
					content: [{ type: "text", text: JSON.stringify(value) }],
				});
			}
		);

		// store a value at the given path in the global kv storage
		this.server.tool(
			"setValue",
			{ path: Array<String>, value: String },
			async ({ path, value }) => {
				const tree = KVTreeStorage(this.env['globaldata']);
				tree.initializeRoot();
				var result = null;
				var replaced = false;
				var stored = true;
				if(tree.nodeExists(path)) {
					result = tree.getValue(path);
					if(result != value) {
						replaced = true;
					} else {
						stored = false;
					}
				}
				if(stored)
				{
					tree.setValue(path, value);
				}
				return ({
					content: [{ type: "text", text: JSON.stringify({'priorValue': result, 'stored': stored, 'replaced': replaced}) }],
				});
			}
		);

		// store a value at the given path in the global kv storage
		this.server.tool(
			"getChildren",
			{ path: Array<String> },
			async ({ path }) => {
				const tree = KVTreeStorage(this.env['globaldata']);
				tree.initializeRoot();
				var result = null;
				if(tree.nodeExists(path)) {
					result = tree.getChildren(path);
				}
				return ({
					content: [{ type: "text", text: JSON.stringify(result) }],
				});
			}
		);

		// Calculator tool with multiple operations
		/*this.server.tool(
			"calculate",
			{
				operation: z.enum(["add", "subtract", "multiply", "divide"]),
				a: z.number(),
				b: z.number(),
			},
			async ({ operation, a, b }) => {
				let result: number;
				switch (operation) {
					case "add":
						result = a + b;
						break;
					case "subtract":
						result = a - b;
						break;
					case "multiply":
						result = a * b;
						break;
					case "divide":
						if (b === 0)
							return {
								content: [
									{
										type: "text",
										text: "Error: Cannot divide by zero",
									},
								],
							};
						result = a / b;
						break;
				}
				return { content: [{ type: "text", text: String(result) }] };
			}
		);
  		*/
	}
}

export default {
	fetch(request: Request, env: Env, ctx: ExecutionContext) {
		const url = new URL(request.url);

		if (url.pathname === "/sse" || url.pathname === "/sse/message") {
			return MyMCP.serveSSE("/sse").fetch(request, env, ctx);
		}

		if (url.pathname === "/mcp") {
			return MyMCP.serve("/mcp").fetch(request, env, ctx);
		}

		return new Response("Not found", { status: 404 });
    },

} satisfies ExportedHandler<{ globaldata: KVNamespace }>;
