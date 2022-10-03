package com.atguigu.bigdata.tune.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderInfo {

    Integer id;
    Long user_id;
    Double total_amount;
    Long create_time;

}
