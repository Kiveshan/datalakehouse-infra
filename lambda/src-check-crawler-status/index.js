const {
  GlueClient,
  GetCrawlerCommand,
} = require("@aws-sdk/client-glue");

const glue = new GlueClient({});

exports.handler = async (event) => {
  const crawlerName = event?.crawlerName || process.env.CRAWLER_NAME;

  if (!crawlerName) {
    throw new Error("crawlerName was not provided in the event or CRAWLER_NAME env var");
  }

  const res = await glue.send(new GetCrawlerCommand({ Name: crawlerName }));
  return {
    crawlerName,
    state: res.Crawler.State, // READY, RUNNING, STOPPING
  };
};
