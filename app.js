const { Client } = require("@elastic/elasticsearch");
const redis = require("redis");
// let dataJson = require("./recordsSampleNew.json");
const index = "nft_test_index";
const express = require("express");
const app = express();
const fs = require("fs");
require("dotenv").config();
const cors = require("cors");
app.use(
	cors({
		origin: "*",
	})
);

const client = new Client({
	node: process.env.ELASTIC_ENDPOINT,
	auth: {
		apiKey: process.env.ELASTIC_API_KEY,
	},
});

const redisClient = redis.createClient({
	password: process.env.REDIS_PASSWORD,
	socket: {
		host: "redis-10706.c323.us-east-1-2.ec2.cloud.redislabs.com",
		port: 10706,
	},
});

//code to process data and create new json file
async function processData() {
	try {
		const newData = await Promise.all(
			dataJson.map(async (data) => {
				if (
					typeof data.properties === "object" &&
					typeof data.token_id === "number"
				) {
					return data;
				}

				if (typeof data.properties !== "object") {
					data.properties = [];
				}

				if (typeof data.token_id !== "number") {
					data.token_id = parseInt(data.token_id["$numberLong"]);
				}

				return data;
			})
		);
		// const dataModified = JSON.stringify(newData);
		fs.writeFileSync("recordsSampleNew2.json", newData, (err) => {
			if (err) {
				console.log(err);
			} else {
				console.log("File written successfully");
			}
		});
	} catch (error) {
		console.error("Error processing data:", error);
	}
}

// processData();

app.get("/ping", async (req, res) => {
	res.send("Server is Running");
});

//code to add data in bulk to elastic search
app.get("/addBulkData", async (req, res) => {
	const bulkBody = [];
	for (const data of dataJson) {
		const documentWithoutId = { ...data };
		documentWithoutId["id"] = documentWithoutId["_id"]["$oid"];
		delete documentWithoutId["_id"];

		bulkBody.push(documentWithoutId);
	}

	try {
		await client.helpers.bulk({
			datasource: bulkBody,
			pipeline: "ent-search-generic-ingestion",
			onDocument: (doc) => ({
				index: { _index: "nft_test_index" },
			}),
		});
	} catch (error) {
		console.error("Error during bulk indexing:", error);
		return res.status(400).send("Failed");
	}
	res.status(200).send("Success");
});

//middleware to get data from redis cache
async function cache(req, res, next) {
	const { nftName, other } = req.query;
	let data;
	if (nftName) {
		data = await redisClient.get("name_" + nftName);
	} else if (other) {
		data = await redisClient.get("other_" + other);
	}
	if (data) {
		res.send(JSON.parse(data));
	} else {
		next();
	}
}

//code to get data from elastic search and store in redis cache
app.get("/getData", cache, async (req, res) => {
	const { nftName, other } = req.query;
	if (nftName) {
		try {
			//remove hyphens and add spaces
			const data = await client.search({
				index: index,
				body: {
					query: {
						match: {
							token_name: {
								query: nftName,
							},
						},
					},
				},
			});
			res.send(data);
			await redisClient.setEx("name_" + nftName, 3600, JSON.stringify(data));
		} catch (error) {
			console.log(error);
			res.status(400).send("Failed");
		}
	} else if (other) {
		try {
			const data = await client.search({
				index: index,
				body: {
					query: {
						multi_match: {
							query: other,
							fields: "*",
						},
					},
				},
			});
			res.send(data);
			await redisClient.setEx("other_" + other, 3600, JSON.stringify(data));
		} catch (error) {
			console.log(error);
			res.status(400).send("Failed");
		}
	}
	// await redisClient.set(nftName, JSON.stringify(data), "EX", 60);
	// res.send(data);
	//code to retrieve data from elastic search
});

//code to get all data from elastic search
app.get("/getall", async (req, res) => {
	const getAll = await client.search({
		index: index,
		body: {
			query: {
				match_all: {},
			},
			size: 1000,
		},
	});
	return res.send(getAll.hits.hits);
});

app.listen(5000, async () => {
	console.log("server started");
	await redisClient.connect();
	console.log("redis connected");
});
