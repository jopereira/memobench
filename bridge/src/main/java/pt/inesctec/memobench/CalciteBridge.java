package pt.inesctec.memobench;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.test.catalog.MockCatalogReaderSimple;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CalciteBridge {
    private final VolcanoPlanner memo;
    private final RelOptCluster cluster;
    private final RelTraitSet set;
    private final @NonNull BridgeCatalogReader cr;

    static AtomicInteger matches;

    public CalciteBridge() {
        memo = new VolcanoPlanner();
        memo.addRelTraitDef(ConventionTraitDef.INSTANCE);

        SqlTypeFactoryImpl tf = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        cr = BridgeCatalogReader.create(tf, false);

        cluster = RelOptCluster.create(memo, new RexBuilder(tf));
        set = RelTraitSet.createEmpty().plus(Convention.NONE);

        memo.addRule(BridgeRule.Config.DEFAULT.toRule());
    }

    public RelSubset addScan(int id, RelNode equiv) {
        String name = String.valueOf(id);
        cr.ensureTable(name);
        RelNode node = new LogicalTableScan(cluster, set, Collections.emptyList(), cr.getTable(List.of(name)));
        return memo.register(node, equiv);
    }

    public RelSubset addJoin(int id, RelNode left, RelNode right, RelNode equiv) {
        RelNode node = new LogicalJoin(cluster, set, Collections.emptyList(), left, right, cluster.getRexBuilder().makeLiteral(String.valueOf(id)), Collections.emptySet(), JoinRelType.INNER, false, ImmutableList.of());
        return memo.register(node, equiv);
    }

    public RelSubset addFilter(int id, RelNode input, RelNode equiv) {
        RelNode node = new LogicalFilter(cluster, set, input, cluster.getRexBuilder().makeLiteral(String.valueOf(id)), ImmutableSet.of());
        return memo.register(node, equiv);
    }

    public int[] getSet(RelSubset subset) {
        RelSubset merged = memo.register(subset.getRelList().get(0), null);

        List<RelNode> list = merged.getRelList();
        int[] result = new int[list.size()];
        int i = 0;
        for (RelNode rel : list) {
            int id;
            if (rel instanceof LogicalTableScan) {
                LogicalTableScan scan = (LogicalTableScan) rel;
                RelOptTable table = scan.getTable();
                id = Integer.parseInt(table.getQualifiedName().get(2));
            } else {
                RexLiteral cond;
                if (rel instanceof LogicalJoin) {
                    LogicalJoin join = (LogicalJoin) rel;
                    cond = (RexLiteral) join.getCondition();
                } else /*if (rel instanceof LogicalFilter)*/ {
                    LogicalFilter filter = (LogicalFilter) rel;
                    cond = (RexLiteral) filter.getCondition();
                }
                id = Integer.parseInt(cond.getValueAs(String.class));
            }
            result[i++] = id;
        }
        return result;
    }

    public void setRoot(RelNode root) {
        memo.setRoot(root);
    }

    public int match() {
        try {
            matches = new AtomicInteger(0);
            memo.findBestExp();
        } catch (RelOptPlanner.CannotPlanException e) {
            // expected
        }
        return matches.get();
    }
}

class BridgeCatalogReader extends MockCatalogReaderSimple {
    private MockSchema mockSchema = new MockSchema("SALES");
    
    private InitializerExpressionFactory expfact = new NullInitializerExpressionFactory();

    protected BridgeCatalogReader(RelDataTypeFactory typeFactory, boolean caseSensitive) {
        super(typeFactory, caseSensitive);
    }

    public static @NonNull BridgeCatalogReader create(RelDataTypeFactory typeFactory, boolean caseSensitive) {
        return new BridgeCatalogReader(typeFactory, caseSensitive).init();
    }

    @Override
    public BridgeCatalogReader init() {
        return (BridgeCatalogReader) super.init();
    }

    void ensureTable(String id) {
        if (getTable(List.of(id)) == null) {
            MockTable empTable = MockTable.create(this, mockSchema, id, false, 10, null, expfact, false);
            registerTable(empTable);
        }
    }
}

