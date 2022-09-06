interface EventSourceRequestMapping {
  method: any,
  headers: {[key: string]: string},
  body: any,
  path: string
}

export default ({ event }: {event: any}): EventSourceRequestMapping =>  {
  console.log('zzzzzzzzzzz', event);
  return {
    headers: event.headers,
    method: event.httpMethod,
    body: event.body,
    path : event.path,
  };
};
