import { routes } from '@stricjs/app';

export function main() {
    const protonIngestEndpoint = `http://${process.env.HOST}:3218/proton/v1/ingest/streams/${process.env.STREAM}`;
    return routes()
        .post('/', c => {
            return c.text().then(a => {
                return fetch(protonIngestEndpoint, {
                    method: "POST",
                    body: `{"columns": ["raw"],"data": [["${a.replaceAll('"', '\\\"')}"]]}`,
                    headers: { "Content-Type": "application/json" },
                }).then(protonResp => new Response('status code ' + protonResp.status));
            });
        })
}