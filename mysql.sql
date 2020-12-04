  SELECT ch.channel_class, c.cust_city, t.calendar_quarter_desc,
     SUM(s.amount_sold) sales_amount
  FROM sales s, times t, customers c, channels ch
  WHERE s.time_id = t.time_id
  AND   s.cust_id = c.cust_id
  AND   s.channel_id = ch.channel_id
  AND   c.cust_state_province = 'CA'
  AND   ch.channel_desc in ('Internet','Catalog')
  AND   t.calendar_quarter_desc IN ('1999-01','1999-02')
 GROUP BY ch.channel_class, c.cust_city, t.calendar_quarter_desc
/
