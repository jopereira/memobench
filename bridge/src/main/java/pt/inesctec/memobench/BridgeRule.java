package pt.inesctec.memobench;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

@Value.Enclosing
public class BridgeRule extends RelRule<RelRule.Config> implements TransformationRule {

    @Value.Immutable(singleton = true)
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableBridgeRule.Config.of()
                .withOperandSupplier(b0 ->
                        b0.operand(LogicalFilter.class).oneInput(b1 ->
                                b1.operand(LogicalJoin.class).anyInputs()));

        @Override
        default BridgeRule toRule() {
            return new BridgeRule(this);
        }
    }

    protected BridgeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        Join join = call.rel(1);
        CalciteBridge.matches.incrementAndGet();
    }
}
