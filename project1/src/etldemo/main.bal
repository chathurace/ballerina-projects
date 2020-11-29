import ballerinax/aws.s3 version 0.11.0;
import ballerinax/googleapis.sheets4 version 0.13.0;
import ballerina/io;
import ballerina/log;
import ballerinax/java.jdbc;
import samjs/jregex;
import ballerina/lang.'int;
import ballerina/kubernetes;

s3:ClientConfiguration amazonS3Config = {
    accessKeyId: "<access-key-id>",
    secretAccessKey: "<access-key>",
    region: "us-west-2"
};

jdbc:Client warehouseDB = new ({
    url: "jdbc:mysql://<dbhost>:<port>/<db-name>",
    username: "<db-user>",
    password: "<db-user-password>",
    dbOptions: {useSSL: false}
});

sheets4:SpreadsheetConfiguration spreadsheetConfig = {
    oauth2Config: {
        accessToken: "<access-token>",
        refreshConfig: {
            clientId: "<client-id>",
            clientSecret: "<client-secret>",
            refreshUrl: "https://oauth2.googleapis.com/token",
            refreshToken: "<refresh-token>"
        }
    }
};

string spreadSheetURL = "https://docs.google.com/spreadsheets/d/<spreadseet-id>";

string inputBucketName = "<input-bucket-name>";
string processedBucketName = "<output-bucket-name>";

map<string> cities = {};

type orderType string|int;

type City record {
    string cityName;
    string postcode;
};

@kubernetes:Job {
    name: "etl-job",
    schedule: "30 23 * * *",
    image: "etlprocess:v.1.0"
}

public function main() {
    
    log:printInfo("Starting the ETL process...");

    log:printInfo("Fetching cities from the database...");
    var cityTable = warehouseDB->select("SELECT city_name as cityName, postcode FROM cities", City);
    if (cityTable is table<City>) {
        foreach City c in cityTable {
            cities[c.postcode] = <@untained>c.cityName;
        }
    } else {
        log:printError("Error in getting city data.");
        return;
    }

    s3:AmazonS3Client|error amazonS3Client = new(amazonS3Config);
    sheets4:Client spreadsheetClient = new (spreadsheetConfig);
    sheets4:Spreadsheet|error spreadsheet = spreadsheetClient->openSpreadsheetByUrl(spreadSheetURL);
    
    if (amazonS3Client is s3:AmazonS3Client && spreadsheet is sheets4:Spreadsheet) {
        log:printInfo("Successfully initialized AWS S3 and Google Sheets connectors.");
        var eSheet = spreadsheet.getSheetByName("Invalid");
        var metaSheet = spreadsheet.getSheetByName("Metadata");
        if (eSheet is sheets4:Sheet && metaSheet is sheets4:Sheet) {
            processData(amazonS3Client, warehouseDB, eSheet, metaSheet);
        }
    }
    log:printInfo("Completed the ETL process.");
}

public function processData(s3:AmazonS3Client amazonS3Client, jdbc:Client warehouseDB, sheets4:Sheet eSheet, sheets4:Sheet mataSheet) {
    
    // Get the starting position of invalid records from the metadata sheet
    // This position is updated in each execution of the ETL flow by adding the number of invalid records found during the execution
    int errorStart = 1;
    var errorStartRow = mataSheet->getCell("A1");
    if (errorStartRow is string) {
        var o = 'int:fromString(errorStartRow);
        if (o is int) {
            errorStart = o;
        }
    }

    // Get the list of all objects in the S3 input bucket and process them
    var objectList = amazonS3Client->listObjects(inputBucketName);
    if (objectList is s3:S3Object[]) {
        log:printInfo("Number of files to process: " + objectList.length().toString());
        foreach var s3Object in objectList {
            string? oName = s3Object["objectName"];
            if (oName is string) {
                log:printInfo("Processing input file: " + oName);
                var s3ContentObject = amazonS3Client->getObject(inputBucketName, <@untainted>oName);
                if (s3ContentObject is s3:S3Object) {
                    byte[]? byteArray = s3ContentObject["content"];
                    if (byteArray is byte[]) {
                        var contentChannel = io:createReadableChannel(byteArray);
                        if (contentChannel is io:ReadableByteChannel) {
                            io:ReadableCharacterChannel cChannel = new (contentChannel, "UTF-8");
                            io:ReadableTextRecordChannel recordChannel = new (cChannel, ",", "\\n");

                            orderType[][] validRecords = [];
                            string[][] invalidRecords = [];
                            var e = processRecords(oName, recordChannel, validRecords, invalidRecords);

                            jdbc:BatchUpdateResult retBatch = warehouseDB->batchUpdate(
                                "INSERT INTO warehouse_orders (item_id, item_count, order_date, store_id, city, postcode) values (?,?,?,?,?,?)", false, ...validRecords);
                            error? dbError = retBatch.returnedError;
                            if (dbError is error) {
                                log:printError("Failed to update database with validated records:" + <string>dbError.detail()?.message);
                                continue;
                            } 
                            log:printDebug("Inserted validated data to the database - " + warehouseDB.toString());

                            sheets4:Range range = {a1Notation: "A" + errorStart.toString(), values: invalidRecords};
                            var r = eSheet->setRange(range);
                            errorStart += invalidRecords.length();
                            log:printDebug("Inserted invalid data to the spreadsheet - " + eSheet.toString());

                            s3:ConnectorError? createStatus = 
                                amazonS3Client->createObject(processedBucketName, <@untainted>oName, <@untainted>byteArray);
                            if (createStatus is s3:ConnectorError) {
                                log:printError("Failed to create backup file object in bucket: " + processedBucketName);
                            } else {
                                log:printInfo("Deleting processed file " + oName + " from the input bucket.");
                                s3:ConnectorError? deleteStatus = amazonS3Client->deleteObject(inputBucketName, <@untained>oName);
                                if (deleteStatus is s3:ConnectorError) {
                                    log:printError("Failed to delete processed file: " + oName);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    sheets4:Range errorCountRange = {a1Notation: "A1", values: [[errorStart]]};
    var ret = mataSheet->setRange(errorCountRange);
    if (ret is error) {
        log:printError("Failed to update error count in metadata. " + ret.reason());
    } else {
        log:printDebug("Updated error count to " + errorStart.toString());
    }
}


function processRecords(string inputSource, io:ReadableTextRecordChannel inputChannel, @tainted orderType[][] processed, @tainted string[][] errors) returns @tainted error? {
    jregex:Pattern pidRegex = jregex:compile(regex = "^[A-Z]_\\d{3}");
    jregex:Pattern dateRegex = jregex:compile(regex = "^\\d{1,2}-\\d{1,2}-\\d{4}");
    jregex:Pattern sidRegex = jregex:compile(regex = "^S_\\d{2}");
    while (inputChannel.hasNext()) {
        var orderRecord = inputChannel.getNext();
        if (orderRecord is string[]) {
            log:printDebug("Processing record: " + orderRecord.toString());
            if (!orderRecord[0].startsWith("#")) {                
                log:printDebug("Record: " + orderRecord.toString());
                if (orderRecord.length() != 5) {
                    orderRecord.unshift("Record should have 5 fields.");
                    orderRecord.unshift(inputSource);
                    errors.push(orderRecord);
                    continue;    
                }
                int|error quantity = 'int:fromString(orderRecord[1]);
                if (quantity is error) {
                    orderRecord.unshift("Second field (order quantity) must be an integer.");
                    orderRecord.unshift(inputSource);
                    errors.push(orderRecord);
                    continue;
                }
                if (!pidRegex.matcher(input = orderRecord[0]).matches()) {  
                    orderRecord.unshift("Item ID is not valid");            
                    orderRecord.unshift(inputSource);          
                    errors.push(orderRecord);
                    continue;
                }
                if (!dateRegex.matcher(input = orderRecord[2]).matches()) {
                    orderRecord.unshift("Order date format is not valid");                      
                    orderRecord.unshift(inputSource);
                    errors.push(orderRecord);
                    continue;                            
                }
                string? cityName = cities[orderRecord[3]];
                if (cityName is string) {
                    orderRecord.push(cityName);
                } else {
                    orderRecord.unshift("Postcode is not valid.");
                    orderRecord.unshift(inputSource);
                    errors.push(orderRecord);
                    continue;
                }
                if (!sidRegex.matcher(input = orderRecord[4]).matches()) {
                    orderRecord.unshift("Store ID is not valid");                      
                    orderRecord.unshift(inputSource);
                    errors.push(orderRecord);
                    continue;                            
                }
                orderType[] validOrder = [orderRecord[0], <int>quantity, orderRecord[2], orderRecord[4], orderRecord[5], orderRecord[3]];
                processed.push(validOrder);
            }
        }
    }
    return;
}

