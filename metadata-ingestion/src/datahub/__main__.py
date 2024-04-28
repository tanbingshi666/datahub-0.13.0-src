from datahub.entrypoints import main

'''
datahub 执行摄取元数据入口 一般情况下指定如下：
datahub ingest -c mysql_to_datahub_rest.yml
底层调用最终为如下：
python3 -m datahub ingest --config mysql_to_datahub_rest.yml 
'''
if __name__ == "__main__":
    main(prog_name="python3 -m datahub")
