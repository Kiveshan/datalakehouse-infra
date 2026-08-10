const {
  GlueClient,
  StartCrawlerCommand,
} = require("@aws-sdk/client-glue");

const glue = new GlueClient({});

// crawlerName should be aws_glue_crawler.raw.name from this project's
// catalog.tf (defaults to CRAWLER_NAME if the Step Function doesn't pass it).
exports.handler = async (event) => {
  const crawlerName = event?.crawlerName || process.env.CRAWLER_NAME;

  if (!crawlerName) {
    throw new Error("crawlerName was not provided in the event or CRAWLER_NAME env var");
  }

  await glue.send(new StartCrawlerCommand({ Name: crawlerName }));
  return { crawlerName, status: "started" };
};
