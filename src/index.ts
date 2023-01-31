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
    let indexStateStr = pathname.split('/')[1]
    if (indexStateStr === '')
      return new Response('bla')

    const cacheUrl = new URL(request.url);

    // Construct the cache key from the cache URL
    const cacheKey = new Request(cacheUrl.toString(), request);
    const cache = caches.default;

    // Check whether the value is already available in the cache
    // if not, you will need to fetch it from origin, and store it in the cache
    const cachedResponse = await cache.match(cacheKey);

    if (cachedResponse)
      return cachedResponse;

    console.log(
      `Response for request url: ${request.url} not present in cache. Computing and caching request.`
    );

    const { value, metadata } = await env.trailers.getWithMetadata(indexStateStr, { type: "arrayBuffer" });
    if (value == null)
      return new Response("index-state not found", { status: 404 })

    const prefix_size = metadata.prefix_size;

    const hackageResponse = await fetch('https://hackage.haskell.org/01-index.tar.gz', {
      headers: {
        Range: `bytes=0-${prefix_size - 1}`
      }
    });

    if (hackageResponse.status != 206)
      return new Response(`hackage says ${hackageResponse.status} while requesting range 0-${prefix_size - 1}`, { status: hackageResponse.status })

    const blob = new Blob([value])

    const response_total_length = prefix_size + blob.size

    let { readable, writable } = new FixedLengthStream(response_total_length);

    if (hackageResponse.body === null)
      return new Response("hackage?", { status: 500 })

    hackageResponse.body.pipeTo(writable, { preventClose: true }).then(() => {
      blob.stream().pipeTo(writable);
    })

    const response = new Response(readable);

    response.headers.append('Cache-Control', 's-maxage=604800, immutable');

    ctx.waitUntil(cache.put(cacheKey, response.clone()));

    return response;
  }
};
