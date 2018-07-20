create table if not exists odata.stock_asset_holding(
trade_id	string  comment '客户代码',
secu_acc_id	string  comment '股票账户',
prd_no	string  comment '资产代码',
sys_code	string  comment '系统代码',
qty	int  comment '持仓量',
prsnt_qty	int  comment '当前持仓量',
asset_amend_qty	int  comment '资产修正量',
mkt_val	float  comment '市值',
last_price	float  comment '考虑了除权的收盘价',
undivi_last_price	float  comment '收盘价',
scale_factor	float  comment '除权因子',
)
partitioned by (busi_date	string  comment '交易日期')
STORED AS ORC;

create table if not exists odata.stock_debt_holding(
trade_id  string comment '客户代码',
secu_acc_id  string comment '股票账户',
prd_no  string comment '资产代码',
liab_qty  int comment '负债数量',
prsnt_liab_qty  int comment '当前负债量',
asset_amend_qty  int comment '负债修正量',
mkt_val  float comment '负债市值',
last_price  float comment '考虑了除权的收盘价',
undivi_last_price  float comment '收盘价',
scale_factor  float comment '除权因子'
)
partitioned by (busi_date	string  comment '交易日期')
STORED AS ORC;


create table if not exists odata.stock_cash_flow_detail(
trade_id   string comment '客户代码',
secu_acc_id   string comment '股票账户',
prd_no   string comment '资产代码',
busi_date   string comment '交易日期',
timestamp   bigint comment '交易时间戳',
busi_flag_code   string comment '业务代码',
busi_flag_name   string comment '业务名称',
trd_qty   int comment '交易数量',
trd_cash_flow   float comment '交易金额',
orig_cash_flow   int comment '原始交易金额',
cash_flow_modi_label   float comment '现金流是否修正',
trd_amt   float comment '成交金额',
trd_price   float comment '成交价格',
sys_code   string comment '系统代码',
trd_type   string comment '交易类型',
inner_busi_flag_code   string comment '内部业务代码',
amortize_label   int comment '是否需要先前回溯平摊费用'
)
STORED AS ORC;