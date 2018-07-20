--gen b


from odata.stock_cash_flow_detail
where trd_type='{0}' and busi_date='{1}'




select a.trade_id,a.secu_acc_id,a.prd_no,a.open_date,
       a.trd_detail,b.trd_detail crash_flow_trd_detail,
       c.qty remain_qty,c.mkt_val remain_val
from (select * from fdata.stock_unclose_prd_long_data where busi_date='2017-01-01' ) a
inner join (
            select trade_id,secu_acc_id,prd_no,collect_list(trd_detail_item) trd_detail from(
            select  trade_id,secu_acc_id,prd_no,
                    (str_to_map(concat(
                        'trd_qty:',trd_qty,
                        ',trd_cash_flow:',trd_cash_flow,
                        ',amortize_label:',amortize_label,
                        ',timestamp:',timestamp
                    ),",",":")) trd_detail_item,
                    row_number() over(partition by trade_id,secu_acc_id,prd_no order by timestamp asc) rank
            from odata.stock_cash_flow_detail
            where trd_type='long_related' and busi_date='2017-01-02'
        ) a
        group by trade_id,secu_acc_id,prd_no
    ) b
    on  a.trade_id=b.trade_id  and a.secu_acc_id=b.secu_acc_id  and a.prd_no=b.prd_no
inner join
    odata.stock_asset_holding c
on a.trade_id=c.trade_id  and a.secu_acc_id=c.secu_acc_id  and a.prd_no=c.prd_no
and c.busi_date='2017-01-02'
        "