package com.tablestore.constant;

/**
 * This file created by mengqingyi on 2017-11-16.
 */

/**
 * 该枚举类用于规范和记录表格存储表名,如需添加新表,请务必先 在此添加枚举及说明注释 所有表格存储主键 名称 一律为 ： partitionKey(分区键) parameterKey(参数主键)
 */
public enum CommonTableNameEnum {

    /**
     * (元数据)数据魔盒运营商报告
     */
    ORIGINAL_SJMH_REPORT("originalSjmhReport"),
    /**
     * 测试用表
     */
    SAVE_TEST("saveTest"),
    /**
     * 工作信息修改记录表
     */
    USER_JOB_MODIFY_TRAJECTORY("userJobModifyTrajectory"),
    /**
     * 规则字段采集记录表
     * 主键：userId，loanApplyId
     */
    RULE_ENGINE_DATA("ruleEngineData"),

    /**
     * 训练字段采集记录表
     * 主键：userId，loanApplyId
     */
    TRAIN_ENGINE_DATA("trainEngineData"),

    /**
     * 用户安装的APP（申请时）
     */
    APPLY_INFO_INSTALLED_APP("applyInfoInstalledApp"),
    
    /**
     * 申请时相关信息采集（wifi、app、其他）
     * 主键：userId,loanApplyId
     */
    APPLY_INFO_COLLECTIONS("applyInfoCollections"),
    /**
     *TD反欺诈数据记录
     * 主键:userId,applyId
     */
    TD_FRAUD_DATA("TDFraudData"),
    /**
     * 申请时查询闪蝶信息记录表（userId、loanApplyId）
     * 主键：userId,loanApplyId
     */
    APPLY_SHANDIE_DATA("applyShanDieData");

    // ORIGINAL_SJMH_REPORT(1){};

    private final String name;

    CommonTableNameEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
