odata.stock_asset_holding
trade_id,secu_acc_id,prd_no,busi_date,sys_code,qty,prsnt_qty,asset_amend_qty,mkt_val,last_price,undivi_last_price,scale_factor

stock_unclose_trade_long_cal
trade_id,secu_acc_id,prd_no,busi_date,open_detail
open_detail:
    open_date,
    orig_trd_qty,
    orig_trd_amt,
    trd_qty,
    trd_amt,
    close_amt,
    unclose_qty,
    unclose_amt,
    weighted_term,
    exception_label

--数据B
select a.trade_id,a.secu_acc_id,a.prd_no,a.open_detail,b.trd_detail
from fdata.stock_unclose_trade_cal a
inner join (
  select trade_id,secu_acc_id,prd_no,collect_list(trd_detail_item) trd_detail from(
    select  trade_id,secu_acc_id,prd_no,
        (str_to_map(concat(
        'trd_qty:',trd_qty,
        ',trd_amt:',trd_amt,
        ',trd_date:"',busi_date,'"',
        ',timestamp:',timestamp),",",":")) trd_detail_item,
        row_number() over(partition by trade_id,secu_acc_id,prd_no order by timestamp asc) rank
    from odata.stock_cash_flow_detail
    where trd_type='long_related' and busi_date=T-1
  ) a
  group by trade_id,secu_acc_id,prd_no
) b
on  a.trade_id=b.trade_id  and a.secu_acc_id=b.secu_acc_id  and a.prd_no=b.prd_no
and a.busi_date=T-2




insert into table fdata.stock_unclose_trade_cal partition(busi_date)
select
    trade_id,secu_acc_id,prd_no,busi_date
    array(map('open_date',busi_date,
              'orig_trd_qty',qty,
              'orig_trd_amt',mkt_val,
              'trd_qty',qty,
              'trd_amt',mkt_val,
              'close_amt',0
              'unclose_qty',0
              'unclose_amt',0
              'weighted_term',0
              'exception_label',0
              'return',0)
    ) open_detail
from odata.stock_asset_holding
where busi_date='{0}'


trd_ind = 0
open_ind = 0
while trd_ind < len(trd_detail) and open_ind < len(open_detail):
  y = open_detail[open_ind]
  x = trd_detail[trd_ind]
  if x.qty > 0 and x.amortize_label = 0:
       if open_detail[-1].open_date == T – 1:
            open_detail[-1].trd_qty += x.trd_qty
            open_detail[-1].unclose_qty += x.trd_qty
            open_detail[-1].orig_trd_qty += x.trd_qty
            open_detail[-1].trd_amt += x.trd_cash_flow
            open_detail[-1].orig_trd_amt += x.trd_cash_flow
       else:
            ## 新的交易，open_date=T-1; orig_trd_qty,trd_qty,unclose_qty都等于x.trd_qty; orig_trd_amt,trd_amt都等于x.trd_cash_flow; close_amt,unclose_amt,weighted_term,exception_label都等于0。
            open_detail.append[x.trd_qty, x.trd_cash_flow]
       update_to_zero(x)
  elif x.amortize_label = 1:
       for z in open_detail[open_ind: ]:
            ratio = z.trd_qty / sum(open_detail[open_ind: ].trd_qty)
            z.trd_qty += x.trd_qty * ratio
            z.unclose_qty += x.trd_qty * ratio
            if x.trd_cash_flow >= 0:
                 z.trd_amt += x.trd_cash_flow * ratio
            else:
                 z.close_amt += -1 * x.trd_cash_flow * ratio
       update_to_zero(x)
  elif x.trd_qty <= 0 and x.amortize_label = 0:
       if y.unclose_qty + x.trd_qty >= 0:
           y.unclose_qty += x.trd_qty
           y.close_amt += -x.trd_cash_flow
           y.unclose_amt *= y.unclose_qty / (y.unclose_qty – x.trd_qty)
           trd_ind += 1
           if y.unclose_qty == 0: open_ind += 1
           update_to_zero(x)
       else:
           y.close_amt += -x.trd_cash_flow * (y.unclose_qty / -x.trd_qty)
           x.trd_cash_flow = x.trd_cash_flow * [1-(y.unclose_qty/-x.trd_qty)]
           x.trd_qty += y.unclose_qty
           y.unclose_amt = 0
           y.unclose_qty = 0
           open_ind += 1
## 处理未完的当日交易
if trd_ind < len(trd_detail):
x = trd_detail[trd_ind]
if x.qty > 0 and x.amortize_label = 0:
     if open_detail[-1].open_date == T – 1:
          open_detail[-1].trd_qty += x.trd_qty
          open_detail[-1].unclose_qty += x.trd_qty
          open_detail[-1].orig_trd_qty += x.trd_qty
          open_detail[-1].trd_amt += x.trd_cash_flow
          open_detail[-1].orig_trd_amt += x.trd_cash_flow
     else:
          open_detail.append[x.trd_qty, x.trd_cash_flow]
     update_to_zero(x)
## 处理完结持仓
for z in reverse(open_detail[open_ind: ]):
  if z.unclose_qty <= holding_detail.trd_qty:
       z.unclose_amt = (z.unclose_qty / holding_detail.trd_qty) * holding_detail.mkt_val
      holding_detail.mkt_val -= (z.unclose_qty / holding_detail.trd_qty) * holding_detail.mkt_val
      holding_detail.trd_qty -= z.unclose_qty
  else:
       ## 异常处理
       z.exception_label = 1
       z.close_amt += (z.unclose_qty – holding_detail.trd_qty) / z.trd_qty * z.trd_amt
       z.unclose_qty = holding_detail.trd_qty
       z.unclose_amt = holding_detail.mkt_val
       holding_detail.trd_qty = 0
       holding_detail.mkt_val = 0
## 异常处理
if holding_detail.trd_qty != 0:
 if open_detail[-1].open_date == T – 1:
      open_detail[-1].trd_qty += holding_detail.trd_qty
      open_detail[-1].unclose_qty += holding_detail.trd_qty
      open_detail[-1].unclose_amt += holding_detail.mkt_val
      open_detail[-1].orig_trd_qty += holding_detail.trd_qty
      open_detail[-1].trd_amt += holding_detail.mkt_val
      open_detail[-1].orig_trd_amt += holding_detail.mkt_val
      open_detail[-1].exception_label = 1
 else:
      ## 新的交易，open_date=T-1; orig_trd_qty,trd_qty,unclose_qty都等于holding_detail.trd_qty; orig_trd_amt,trd_amt都等于holding_detail.mkt_val; close_amt,unclose_amt,weighted_term,exception_label都等于0。
      open_detail.append[holding_detail.trd_qty, holding_detail.mkt_val]
      open_detail[-1].exception_label = 1
      open_detail[-1].unclose_amt = holding_detail.mkt_val
## 更新weighted_term
for x in open_detail:
  x.weighted_term += x.unclose_qty / x.trd_qty
