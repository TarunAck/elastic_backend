const { Client } = require("@elastic/elasticsearch");
const { CovalentClient } = require("@covalenthq/client-sdk");
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

//middleware to check if address is valid
const checkAddress = async (req, res, next) => {
	const { address } = req.query;
	const check = await client.search({
		index: index,
		body: {
			query: {
				match: {
					contract_address: {
						query: address,
					},
				},
			},
		},
	});
	if (check.hits.total.value > 0) {
		return res.status(400).send("Address already exists");
	}
	next();
};

const json = (param) => {
	return JSON.stringify(
		param,
		(key, value) => (typeof value === "bigint" ? value.toString() : value) // return everything else unchanged
	);
};

app.get("/addData", async (req, res) => {
	const { address, chainName } = req.query;

	const covalentClient = new CovalentClient(process.env.COVALENT_API_KEY);
	const resp = await covalentClient.NftService.getNftsForAddress(
		chainName,
		address
	);
	if (resp.error) {
		return res.status(400).send("Invalid Address");
	}
	const nfts = JSON.parse(json(resp.data.items));
	if (nfts.length === 0) {
		return res.status(400).send("No NFTs found");
	}
	const bulkDocument = [];
	for (const nft of nfts) {
		console.log(nft);
		for (const nftData of nft.nft_data) {
			const document = {
				token_id: nftData.token_id,
				token_url: nftData.token_url,
				token_name: nftData.external_data?.name,
				image_url: nftData.external_data?.image,
				description: nftData.external_data?.description,
				properties: nftData.external_data?.attributes,
				owner: nftData.original_owner,
				location: "",
				priceURI: "",
				transactionURI: "",
				contract_address: nft.contract_address,
				orgName: nft.contract_ticker_symbol,
			};
			const check = await client.search({
				index: index,
				body: {
					query: {
						match: {
							token_id: {
								query: document.token_id,
							},
						},
					},
				},
			});
			if (check.hits.total.value > 0) {
				console.log("already exists " + document.token_id);
				continue;
			} else {
				bulkDocument.push(document);
			}
			// try {
			// 	await client.index({
			// 		index: index,
			// 		body: document,
			// 	});
			// } catch (error) {
			// 	console.log(error);
			// 	return res.status(400).send("Failed");
			// }
		}
	}
	try {
		await client.helpers.bulk({
			datasource: bulkDocument,
			pipeline: "ent-search-generic-ingestion",
			onDocument: (doc) => ({
				index: { _index: "nft_test_index" },
			}),
		});
	} catch (error) {
		console.error("Error during bulk indexing:", error);
		return res.status(400).send("Failed");
	}

	res.send("added successfully");
});

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
