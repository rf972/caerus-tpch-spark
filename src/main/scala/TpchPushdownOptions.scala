package org.tpch.pushdown.options

/** Represents the options that are possible when we are
 *  performing pushdown operations.
 *
 */
case class TpchPushdownOptions(enableFilter: Boolean,
                               enableProject: Boolean,
                               enableAggregate: Boolean,
                               explain: Boolean) {

    def isPushdownEnabled() : Boolean = {
      enableFilter && enableProject && enableAggregate
    }
}