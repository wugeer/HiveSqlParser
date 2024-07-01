package org.example;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.lib.*;
import org.apache.hadoop.hive.ql.parse.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import java.util.stream.Collectors;

public class HiveSqlParser implements NodeProcessor {

    private static Logger logger = LoggerFactory.getLogger(HiveSqlParser.class);
    private static Context context = null;
    private final static String HDFS_SESSION_PATH_KEY = "_hive.hdfs.session.path";
    private final static String LOCAL_SESSION_PATH_KEY = "_hive.local.session.path";

    private static String hdfsTemporaryDirectory(HiveConf hiveConf) {
        return hiveConf.get("hadoop.tmp.dir", "/tmp");
    }

    private static String localTemporaryDirectory() {
        return System.getProperty("java.io.tmpdir", "/tmp");
    }

    static {
        HiveConf hiveConf = new HiveConf();
        if (hiveConf.get(HDFS_SESSION_PATH_KEY) == null) {
            hiveConf.set(HDFS_SESSION_PATH_KEY, hdfsTemporaryDirectory(hiveConf));
        }
        if (hiveConf.get(LOCAL_SESSION_PATH_KEY) == null) {
            hiveConf.set(LOCAL_SESSION_PATH_KEY, localTemporaryDirectory());
        }
        try {
            context = new Context(hiveConf);
        } catch (IOException e) {
            logger.error("Init hive context fail, message: " + e);
        }
    }

    List<String> tableNames = new ArrayList<>();
    Set<String> cteNames = new HashSet<>();

    public void parse(String query) throws ParseException, SemanticException {
        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(query, context);
        logger.info("start to analyze query: {}, ASTNode: {}", query, tree.dump());

        Map<Rule, NodeProcessor> rules = Maps.newLinkedHashMap();
        Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
        GraphWalker ogw = new DefaultGraphWalker(disp);
        final List<Node> topNodes = Lists.newArrayList(tree);

        // Traverse the AST
        ogw.startWalking(topNodes, null);
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        if (!(nd instanceof ASTNode)) {
            return null;
        }

        ASTNode pt = (ASTNode) nd;
        int type = pt.getType();

        switch (type) {
            case HiveParser.TOK_CREATETABLE:
                extractTableNameFromCreateTable(pt);
                break;
            case HiveParser.TOK_TAB:
                extractTableNameFromInsert(pt);
                break;
            case HiveParser.TOK_QUERY:
                extractTableNamesFromQuery(pt);
                break;
            case HiveParser.TOK_CTE:
                extractTableNamesFromWithClause(pt);
                break;
            default:
                // Handle other types of nodes if needed
                break;
        }
        return null;
    }

    private void extractTableNameFromCreateTable(ASTNode pt) {
        for (Node child : pt.getChildren()) {
            ASTNode createTableChild = (ASTNode) child;
            if (createTableChild.getToken().getType() == HiveParser.TOK_TABNAME) {
                String tableName = BaseSemanticAnalyzer.getUnescapedName(createTableChild);
                if (!tableNames.contains(tableName)) {
                    tableNames.add(tableName);
                }
            } else if (createTableChild.getToken().getType() == HiveParser.TOK_QUERY) {
                // This is a CTAS statement
                extractTableNamesFromQuery(createTableChild);
            }
        }
    }

    private void extractTableNameFromInsert(ASTNode pt) {
        for (Node node : pt.getChildren()) {
            ASTNode insertNode = (ASTNode) node;
            if (insertNode.getToken().getType() == HiveParser.TOK_TAB) {
                String tableName = BaseSemanticAnalyzer.getUnescapedName(insertNode);
                if (!tableNames.contains(tableName)) {
                    tableNames.add(tableName);
                }
            }
        }
    }

    private void extractTableNamesFromQuery(ASTNode pt) {
        if (pt == null) return;

        switch (pt.getToken().getType()) {
            case HiveParser.TOK_SELECT:
                extractTableNamesFromSelect(pt);
                break;
            case HiveParser.TOK_SUBQUERY:
                extractTableNamesFromQuery((ASTNode) pt.getChild(0)); // handle subquery
                break;
            case HiveParser.TOK_JOIN:
            case HiveParser.TOK_LEFTOUTERJOIN:
            case HiveParser.TOK_RIGHTOUTERJOIN:
            case HiveParser.TOK_FULLOUTERJOIN:
                for (Node child : pt.getChildren()) {
                    extractTableNamesFromQuery((ASTNode) child); // handle joins
                }
                break;
            case HiveParser.TOK_UNIONALL:
                for (Node child : pt.getChildren()) {
                    extractTableNamesFromQuery((ASTNode) child); // handle UNION ALL
                }
                break;
            case HiveParser.TOK_QUERY:
                // For TOK_QUERY node, process its children to extract table names
                for (Node child : pt.getChildren()) {
                    extractTableNamesFromQuery((ASTNode) child);
                }
                break;
            case HiveParser.TOK_FROM:
                for (Node child : pt.getChildren()) {
                    if (child instanceof ASTNode && ((ASTNode) child).getToken().getType() == HiveParser.TOK_TABREF) {
                        extractTableNamesFromTabref((ASTNode) child);
                    } else {
                        extractTableNamesFromQuery((ASTNode) child); // Handle other types of FROM clauses
                    }
                }
                break;
            case HiveParser.TOK_TABREF:
                extractTableNamesFromTabref(pt);
                break;
            case HiveParser.TOK_LATERAL_VIEW:
                for (Node child : pt.getChildren()) {
                    extractTableNamesFromQuery((ASTNode) child); // Recursively process children of TOK_LATERAL_VIEW
                }
                break;
            default:
                // Handle other cases if needed
                break;
        }
    }

    private void extractTableNamesFromTabref(ASTNode tabref) {
        ASTNode tableTree = (ASTNode) tabref.getChild(0);
        String tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) tableTree.getChild(0));
        if (tableTree.getChildCount() > 1) {
            tableName = tableName + "." + tableTree.getChild(1);
        }
        if (!tableNames.contains(tableName) && !cteNames.contains(tableName)) {
            tableNames.add(tableName);
        }
    }

    private void extractTableNamesFromSelect(ASTNode pt) {
        if (pt == null) return;

        // Find the TOK_TABREF node under TOK_SELECT node
        for (Node child : pt.getChildren()) {
            ASTNode selectChild = (ASTNode) child;
            if (selectChild.getToken().getType() == HiveParser.TOK_SELEXPR) {
                extractTableNamesFromSelectExpr(selectChild);
            } else if (selectChild.getToken().getType() == HiveParser.TOK_WHERE) {
                extractTableNamesFromWhere(selectChild);
            }
        }
    }

    private void extractTableNamesFromSelectExpr(ASTNode pt) {
        if (pt == null) return;

        // TOK_SELEXPR node contains expressions, including references to tables
        for (Node child : pt.getChildren()) {
            ASTNode exprNode = (ASTNode) child;
            if (exprNode.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
                ASTNode tableRef = (ASTNode) exprNode.getChild(0);
                if (tableRef != null && tableRef.getToken() != null) {
                    String tableName = BaseSemanticAnalyzer.getUnescapedName(tableRef);
                    if (!tableNames.contains(tableName) && !cteNames.contains(tableName)) {
                        tableNames.add(tableName);
                    }
                }
            }
        }
    }

    private void extractTableNamesFromWhere(ASTNode pt) {
        if (pt == null) return;

        // TOK_WHERE node may contain conditions referencing tables
        for (Node child : pt.getChildren()) {
            ASTNode whereExpr = (ASTNode) child;
            extractTableNamesFromWhereExpr(whereExpr);
        }
    }

    private void extractTableNamesFromWhereExpr(ASTNode pt) {
        if (pt == null) return;

        // Recursive traversal to find table references in WHERE condition expressions
        for (Node child : pt.getChildren()) {
            if (child instanceof ASTNode) {
                ASTNode exprNode = (ASTNode) child;
                if (exprNode.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
                    ASTNode tableRef = (ASTNode) exprNode.getChild(0);
                    if (tableRef != null && tableRef.getToken() != null) {
                        String tableName = BaseSemanticAnalyzer.getUnescapedName(tableRef);
                        if (!tableNames.contains(tableName) && !cteNames.contains(tableName)) {
                            tableNames.add(tableName);
                        }
                    }
                }
                extractTableNamesFromWhereExpr(exprNode); // Recursive call for nested expressions
            }
        }
    }

    private void extractTableNamesFromWithClause(ASTNode pt) {
        for (Node child : pt.getChildren()) {
            ASTNode cteNode = (ASTNode) child;
            if (cteNode.getToken().getType() == HiveParser.TOK_SUBQUERY) {
                ASTNode aliasNode = (ASTNode) cteNode.getChild(1);
                if (aliasNode != null && aliasNode.getToken().getType() == HiveParser.Identifier) {
                    String cteName = BaseSemanticAnalyzer.getUnescapedName(aliasNode);
                    cteNames.add(cteName); // Add CTE name to the set
                    extractTableNamesFromQuery((ASTNode) cteNode.getChild(0)); // Process the CTE query
                }
            }
        }
    }

    public List<String> getTableNames() {
        return tableNames.stream()
                .filter(name -> !cteNames.contains(name))
                .collect(Collectors.toList());
    }

    public static void reinitializeContext() {
        HiveConf hiveConf = new HiveConf();
        if (hiveConf.get(HDFS_SESSION_PATH_KEY) == null) {
            hiveConf.set(HDFS_SESSION_PATH_KEY, hdfsTemporaryDirectory(hiveConf));
        }
        if (hiveConf.get(LOCAL_SESSION_PATH_KEY) == null) {
            hiveConf.set(LOCAL_SESSION_PATH_KEY, localTemporaryDirectory());
        }
        try {
            context = new Context(hiveConf);
        } catch (IOException e) {
            logger.error("Init hive context fail, message: " + e);
        }
    }

    public static void main(String[] args) {
        try {
            HiveSqlParser processor = new HiveSqlParser();
            processor.parse("with cte_table as (select id, name from test.my_table) select * from cte_table");
            System.out.println("Tables: " + processor.getTableNames());
        } catch (ParseException | SemanticException e) {
            e.printStackTrace();
        }
    }
}
