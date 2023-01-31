export interface Env {
  trailers: KVNamespace;
}

async function MethodNotAllowed(method: string) {
  return new Response(`Method ${method} not allowed.`, {
    status: 405,
    headers: {
      Allow: 'GET',
    },
  });
}

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext
  ): Promise<Response> {
    if (request.method !== 'GET')
      return MethodNotAllowed(request.method);

    let { pathname } = new URL(request.url);
    let index_state_str = pathname.split('/')[1]
    if (index_state_str === '')
      return new Response('bla')

    const { value, metadata } = await env.trailers.getWithMetadata(index_state_str, { type: "arrayBuffer" });
    if (value == null)
      return new Response("index-state not found", { status: 404 })

    const prefix_size = metadata.prefix_size;

    let response = await fetch('https://hackage.haskell.org/01-index.tar.gz', {
      headers: {
        Range: `bytes=0-${prefix_size - 1}`
      }
    });

    if (response.status != 206)
      return new Response(`hackage says ${response.status} while requesting range 0-${prefix_size - 1}`, { status: response.status })

    const blob = new Blob([value])

    const response_total_length = prefix_size + blob.size

    let { readable, writable } = new FixedLengthStream(response_total_length);

    if (response.body === null)
      return new Response("hackage?", { status: 500 })

    response.body.pipeTo(writable, { preventClose: true }).then(() => {
      blob.stream().pipeTo(writable);
    })

    return new Response(readable);
  }
};
