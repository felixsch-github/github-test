package ie.thinkevolvesolve.edam

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import ie.thinkevolvesolve.edam.iterator.BatchIterator
import ie.thinkevolvesolve.edam.json.JsonOutRowsReader
import ie.thinkevolvesolve.edam.json.TesFileUtils

import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.time.Instant

@Slf4j
class AssetMirrorHelper {

    Connection sourceConnection
    Connection targetConnection

    String tableName
    String schemaName
    String schemaAndTableName

    def fieldList

    Integer batchSize = 100

    Sql sourceSql
    Sql targetSql

    String insertSql

    JsonSlurper slurper = new JsonSlurper()

    void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize
    }

    void setSourceConnection(Connection sourceConnection) {
        this.sourceConnection = sourceConnection
        sourceSql = new Sql(sourceConnection)
    }

    void setFieldList(def fieldList) {
        this.fieldList = fieldList
        insertSql = "INSERT INTO $schemaAndTableName (asset_id,"
        fieldList.each { f ->
            insertSql = insertSql + f.name + (f != fieldList.last() ? "," : "")
        }
        insertSql = insertSql + ") VALUES (?," + ("?," * fieldList.size()).replaceAll(",\$", "") + ")"
    }

    AssetMirrorHelper(
            Connection targetConnection,
            Enterprises enterprise,
            DataStores datastore,
            String targetSchemaName
    ) {
        this.targetConnection = targetConnection

        targetSql = new Sql(targetConnection)

        this.fieldList = fieldList

        tableName = datastore.getDatabaseTableName()

        if (targetSchemaName == null || targetSchemaName.isEmpty()) {
            schemaName = enterprise.getDatabaseSchemaName()
        } else {
            schemaName = targetSchemaName
        }

        log.info("Schema Name = {}", schemaName)

        if (enterprise.dataSourceName) {
            if (targetSchemaName == null || targetSchemaName.isEmpty()) {
                schemaName = null
                schemaAndTableName = tableName
            } else {
                schemaAndTableName = schemaName + "." + tableName
            }
        } else if (schemaName) {
            schemaAndTableName = schemaName + "." + tableName
            if (!schemaExists()) {
                log.info("Schema $schemaName does not exist")
                throw new RuntimeException("Schema $schemaName does not exist")
            }
        } else {
            return
        }

        if (!tableExists()) {
            log.info("Schema/Table $schemaAndTableName does not exist")
            throw new RuntimeException("Schema/Table $schemaAndTableName does not exist")
        }

    }

    def commit() {
        targetSql.commit()
    }

    def unMirrorAsset(Assets asset) {

        log.debug("DELETE FROM " + schemaAndTableName + " WHERE asset_id = :assetId", ["assetId": asset.id])
        targetSql.execute("DELETE FROM " + schemaAndTableName + " WHERE asset_id = :assetId", ["assetId": asset.id])

        log.debug("DELETE FROM " + schemaAndTableName + "_metadata WHERE asset_id = :assetId", ["assetId": asset.id])
        targetSql.execute("DELETE FROM " + schemaAndTableName + "_metadata WHERE asset_id = :assetId", ["assetId": asset.id])

    }

    def mirrorAsset(Assets asset) {

        targetSql.execute("DELETE FROM " + schemaAndTableName + "_metadata WHERE asset_id = :assetId", ["assetId": asset.id])
        targetSql.execute(
                "INSERT INTO " + schemaAndTableName + "_metadata (username, asset_id) VALUES ( :username, :assetId )",
                ["username": asset.createdBy.username, "assetId": asset.id]
        )

        targetSql.execute("DELETE FROM " + schemaAndTableName + " WHERE asset_id = :assetId", ["assetId": asset.id])

        def insertRow = []

        def batchIterator

        boolean writeToDiskEnabled = TesFileUtils.writeToDiskEnabled(asset.workspace.id)
        if (writeToDiskEnabled) {
            String jsonOutRowsFilename = TesFileUtils.getJsonOutRowFilename(asset)
            batchIterator = new JsonOutRowsReader(jsonOutRowsFilename)
        } else {
            batchIterator = BatchIterator.createOutRowsIterator(sourceSql, asset.id, asset.outRowsRevision)
        }

        try {

            def updateCounts = targetSql.withBatch(batchSize, insertSql) { ps ->

                while (batchIterator.hasNext()) {

                    def row
                    def resultSet = batchIterator.next()
                    if (writeToDiskEnabled) {
                        row = resultSet.row
                    } else {
                        row = slurper.parseText(resultSet.jsondata)
                    }

                    insertRow = [asset.id]

                    fieldList.eachWithIndex { field, count ->

                        def v = null
                        if (field.columnIndex <= row.size()) {
                            v = row[field.columnIndex]
                        }

                        switch (field.type) {
                            case "date":
                                if (v) {
                                    v = Date.from(Instant.ofEpochMilli(v))
                                } else {
                                    v = null
                                }
                                insertRow.add(v)
                                break
                            case "number":
                            case "string":
                            case "integer":
                                insertRow.add(v)
                                break
                            default:
                                throw new RuntimeException("Unknown field type " + field.type)
                        }

                    }

                    ps.addBatch(insertRow)
                }
            }
            if (writeToDiskEnabled) {
                batchIterator.close()
            }

        } catch (Exception e) {
            log.error("" + e)
            log.error(" SQL " + insertSql)
            log.error(" ROW (#" + insertRow.size() + ") " + JsonOutput.toJson(insertRow))
            log.debug("", e)

            throw e
        }

        targetSql.commit()
    }


    private boolean schemaExists() {
        // Check for the SQL table
        DatabaseMetaData meta = targetConnection.getMetaData()
        ResultSet rs = meta.getSchemas()
        while (rs.next()) {
            if (rs.getString("TABLE_SCHEM").equalsIgnoreCase(schemaName)) {
                log.info("Schema " + schemaName + " exists")
                return true
            }
        }
        return false
    }


    private boolean tableExists() {
        DatabaseMetaData meta = targetConnection.getMetaData()
        if (!tableName) {
            return false
        }
        ResultSet rs = meta.getTables(null, schemaName?.toUpperCase(), tableName.toUpperCase(), null)
        return rs.next()
    }

}
