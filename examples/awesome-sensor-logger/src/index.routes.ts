import { routes } from '@stricjs/app';

export function main() {
    return routes()
        .post('/', c => {
            return c.text().then(a => {
                return fetch("http://localhost:3218/proton/v1/ingest/streams/phone", {
                    method: "POST",
                    body: `{"columns": ["raw"],"data": [["${a.replaceAll('"', '\\\"')}"]]}`,
                    headers: { "Content-Type": "application/json" },
                }).then(protonResp => new Response('status code ' + protonResp.status));
            });
        })
}