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

public class MyProcessor implements NodeProcessor {

    private static Logger logger = LoggerFactory.getLogger(MyProcessor.class);
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

    List<String> tableName = new ArrayList<>();

    public void parse(String query) throws ParseException, SemanticException {
        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(query, context);
        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }
        logger.info("start to analyze query: {}, ASTNode: {}", query, tree.dump());
        Map<Rule, NodeProcessor> rules = Maps.newLinkedHashMap();
        Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
        GraphWalker ogw = new DefaultGraphWalker(disp);
        final List<Node> topNodes = Lists.newArrayList(tree);

        // 遍历
        ogw.startWalking(topNodes, null);
        // 打印
        System.out.println(tableName);
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        ASTNode pt = (ASTNode) nd;
        switch (pt.getToken().getType()) {
            case org.apache.hadoop.hive.ql.parse.HiveParser.TOK_CREATETABLE:
                for (Node node : pt.getChildren()) {
                    ASTNode createTableChild = (ASTNode) node;
                    if (createTableChild.getToken().getType() == HiveParser.TOK_TABNAME) {
                        tableName.add(BaseSemanticAnalyzer.getUnescapedName(createTableChild));
                    }
                }
        }
        return null;
    }
}
