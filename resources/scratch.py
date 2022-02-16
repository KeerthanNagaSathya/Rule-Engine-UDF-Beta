# time_diff = " and max_time - min_time > 30"
'''
where_query = "select id, date, time, txn_source_code, amount, total_amount, is_ttr, ((bigint(" \
              "to_timestamp(" \
              "max_time)))-(bigint(to_timestamp(min_time))))/(60) as time_diff from {}".format(
                table_name) + where_query \
              + "and ((bigint(to_timestamp(max_time)))-(bigint(to_timestamp(min_time))))/(60) <= " \
                "30 order by id "

where_query = where_query + " and ((bigint(to_timestamp(max_time)))-(bigint(to_timestamp(" \
                            "min_time))))/(60) <= " \
                            "30 order by id "

'''