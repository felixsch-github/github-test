package ie.thinkevolvesolve.edam

import com.google.common.base.Enums
import groovy.util.logging.Slf4j
import ie.thinkevolvesolve.edam.exception.ObjectNotFoundException
import org.apache.commons.lang3.StringUtils
import org.hibernate.cfg.ImprovedNamingStrategy

import java.nio.charset.StandardCharsets

@Slf4j
class SearchHelper {

    boolean excludeInactive = true

    static String excludeInactiveEnterprises = """
        AND NOT EXISTS (
            SELECT ew.workspaces_id FROM enterprises_workspaces ew, enterprises e
            WHERE ew.enterprises_workspaces_id = e.id
            AND ew.workspaces_id = ws.id
            AND ( e.status = 'INACTIVE' OR e.status = 'DELETED' )
        )
    """

    ArrayList unionSql = []

    String searchClause = ""

    Class domainClass
    ArrayList sqlParams = []

    String orderBy = "r.id"

    int max
    int offset

    String nameLike

    SearchHelper(def domainClass, boolean excludeInactive = true) {
        this.domainClass = domainClass
        this.excludeInactive = excludeInactive
    }

    def setFilter(String filter) {
        searchClause = ""

        if (!filter) {
            return ''
        }

        //filter is already decoded
        try {
            filter = URLDecoder.decode(filter, StandardCharsets.UTF_8.toString())
        } catch (IllegalArgumentException iae) {
            log.debug("Filter already decoded? '${filter}'")
        }

        //escape % and [
        filter = StringUtils.replace(filter, "[", "[[]", -1)
        filter = StringUtils.replace(filter, "%", "[%]", -1)

        filter.split(' and ').each {

            if (it.contains('=')) {

                def nvpair = it.split('=').each { it.trim() }

                if (nvpair[0] == 'state') {

                    def stateList = []
                    nvpair[1].split(',').each { state ->
                        if (!Enums.getIfPresent(AssetsStateEnum.class, state).isPresent()) {
                            throw new ObjectNotFoundException("'" + state + "' is not a valid state")
                        }
                        stateList.add(state)
                    }

                    // FIXME SQL IN LIST is not working
                    //searchClause += " AND r.asset_state IN ( :stateList )"
                    //sqlParams.add(["stateList", stateList])

                    if (stateList.size()) {
                        def orList = []
                        stateList.eachWithIndex { state, i ->
                            orList.add("r.asset_state = :state" + i)
                            sqlParams.add(["state" + i, state])
                        }
                        searchClause += " AND (" + orList.join(" OR ") + ")"
                    }


                } else if (nvpair[0] == 'id') {
                    searchClause += " AND r.id = :id"
                    sqlParams.add(["id", nvpair[1]])
                } else if (nvpair[0] == 'vstate') {
                    def stateList = []
                    nvpair[1].split(',').each { state ->
                        stateList.add(state.trim())
                    }
                    def orList = []
                    stateList.eachWithIndex { state, i ->
                        orList.add(" v.state = :vstate" + i)
                        sqlParams.add(["vstate" + i, state])
                    }
                    searchClause += " AND (" + orList.join(" OR ") + ")"
                }
            } else if (it.contains(' contains ')) {

                def nvpair = it.split(' contains ').collect { it.trim() }

                String n = nvpair[1].replaceAll("^'", "").replaceAll("'\$", "")
                if (n.length() > 0) {
                    if (domainClass in Users) {
                        searchClause += " AND LOWER(r.username) like LOWER(:name)"
                    } else {
                        searchClause += " AND LOWER(r.name) like LOWER(:name)"
                    }
                    if (!sqlParams.contains(["name", "%" + n + "%"])) {
                        sqlParams.add(["name", "%" + n + "%"])
                        nameLike = n
                    }
                }

            } else if (it == "processed") {

                searchClause += " AND r.asset_state IN ('" + AssetsStateEnum.getProcessed().join("','") + "') "

            } else if (it == "notprocessed") {

                searchClause += " AND r.asset_state IN ('" + AssetsStateEnum.getNotProcessed().join("','") + "') "

            } else if (it == "overdue") {

                searchClause += " AND sa.due_date < CURRENT_TIMESTAMP"

            }
        }
    }

    def setOrderBy(String sort) {

        if (sort) {
            String field = sort.replaceAll("\\s+(asc|desc)\$", "")

            def obj = domainClass.newInstance()

            ImprovedNamingStrategy improvedNamingStrategy = new ImprovedNamingStrategy()

            if (field != "id" && !obj.hasProperty(field)) {
                throw new RuntimeException("Field `" + field + "` is not valid")
            }

            String databaseColumn = improvedNamingStrategy.columnName(field)

            if (databaseColumn != field) {
                if (databaseColumn == "created_by") {
                    databaseColumn = "created_by_id"
                } else if (databaseColumn == "updated_by") {
                    databaseColumn = "updated_by_id"
                } else if (databaseColumn == "assigned_to") {
                    databaseColumn = "assigned_to_id"
                }

                this.orderBy = sort.replaceAll(field, databaseColumn)
            } else {
                this.orderBy = sort.replaceAll(field, databaseColumn)
            }

        }

    }

    def addSqlParam(String name, def value) {
        sqlParams.add([name, value])
    }

    def addSql(String sql) {
        String excludeInactiveEnterprises = this.excludeInactiveEnterprises
        if (domainClass.getName() == "ie.thinkevolvesolve.edam.Workspaces") {
            excludeInactiveEnterprises = excludeInactiveEnterprises.replace("ws.id", "r.id")
        }
        unionSql.add(sql + " " + searchClause + (excludeInactive ? " " + excludeInactiveEnterprises : ''))
    }

    def groupByStateAndOverdue() {

        String sql = unionSql.join(" UNION ")

        String groupSql = """
            SELECT count(r.id) as count, r.asset_state as name, CASE WHEN sa.due_date < CURRENT_TIMESTAMP THEN 1 ELSE 0 END AS overdue
            FROM ( ${sql} ) r, schedule_asset sa
            WHERE r.id = sa.asset_id
            GROUP BY r.asset_state, CASE WHEN sa.due_date < CURRENT_TIMESTAMP THEN 1 ELSE 0 END
        """

        def result

        domainClass.withNewSession { session ->
            def query = session.createSQLQuery(groupSql)

            sqlParams.each {
                query.setParameter(it[0], it[1])
            }

            result = query.list()
        }

        result = result.collect {
            return ['name': it[1], 'count': it[0], 'overdue': it[2] == 1 ? true : false]
        }

        return result
    }

    def getResults() {

        String sql = unionSql.join(" UNION ")

        String countSql = "SELECT count(DISTINCT r.id) FROM ( ${sql} ) r"
        String getSql = "SELECT r.* FROM ( ${sql} ) r ORDER BY $orderBy"

        log.debug("getSql " + getSql)
        log.debug("countSql " + countSql)
        log.debug("sqlParams " + sqlParams)

        def rv = [:]

        domainClass.withNewSession { session ->

            def query = session.createSQLQuery(countSql)

            sqlParams.each {
                query.setParameter(it[0], it[1])
            }

            def recordCount = query.list()[0]

            query = session.createSQLQuery(getSql)
                    .addEntity(domainClass)
                    .setFirstResult(offset)
                    .setMaxResults(max)

            sqlParams.each {
                query.setParameter(it[0], it[1])
            }

            rv = [
                    results: query.list(),
                    count  : recordCount.intValue()
            ]
        }

        // https://github.com/grails/grails-data-mapping/issues/562
        // https://docs.grails.org/3.1.1/ref/Domain%20Classes/attach.html
        rv.results.each { it.attach() }

        return rv
    }

    static def adminListDomain(Class domainClass, Map params) {
        SearchHelper helper = new SearchHelper(domainClass, false)
        helper.setFilter(params.filter)
        helper.setOrderBy(params.sort)
        helper.setMax(params.max)
        helper.setOffset(params.offset)

        String sql = "SELECT r.* FROM ${domainClass.simpleName.toLowerCase()} r"
        if (helper.searchClause) {
            sql += " WHERE 1=1"
        }

        helper.addSql(sql)
        return helper.getResults()
    }

}
