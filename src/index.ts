export interface Env {
  IndexState: KVNamespace;
}

async function MethodNotAllowed(method: string) {
  return new Response(`Method ${method} not allowed.`, {
    status: 405,
    headers: {
      Allow: 'GET',
    },
  });
}

function base64Decode(b: string) {
  b = atob(b);
  const
    length = b.length,
    buf = new ArrayBuffer(length),
    bufView = new Uint8Array(buf);
  for (var i = 0; i < length; i++) { bufView[i] = b.charCodeAt(i) }
  return buf
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

    const value = await env.IndexState.get(index_state_str, { type: "json" });
    if (value === null)
      return new Response("index-state not found", { status: 404 })

    const { prefix_size, trailer } = value;

    let response = await fetch('https://hackage.haskell.org/01-index.tar.gz', {
      headers: {
        Range: `bytes=0-${prefix_size - 1}`
      }
    });

    if (response.status != 206)
      return new Response(`hackage says ${response.status} while requesting range 0-${prefix_size - 1}`, { status: response.status })

    const trailerBlob = new Blob([base64Decode(trailer)])

    const response_total_length = prefix_size + trailerBlob.size

    let { readable, writable } = new FixedLengthStream(response_total_length);

    if (response.body === null)
      return new Response("hackage?", { status: 500 })

    console.log(response_total_length, prefix_size, trailerBlob.size)

    console.log(response.headers)

    response.body.pipeTo(writable, { preventClose: true }).then(() => {
      console.log("there")
      trailerBlob.stream().pipeTo(writable);
    })

    //await response.body.pipeTo(writable, { preventClose: true })
    //trailerBlob.stream().pipeTo(writable);

    return new Response(readable);
  }
};
