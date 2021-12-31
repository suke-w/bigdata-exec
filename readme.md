2021-12-31
sparksql java api 对接.del文件
问题1：
    文件分隔符在linux系统下显示为^B，notepad++中显示为STX，java程序中写为“\002”可转化，放在配置文件中读入为002，后改为
    \u0002可行
    了解unicode以及utf-8编码
问题2：
    .del文件切分后，组装Row时date类型在程序中需由java.sql.Date解析，用java.util.Date解析会报错
        new java.sql.Date(new SimpleDateFormat("yyyy-MM-dd").parse("2020-12-21").getTime)
    完善补充spark解析
----------------------------------------------------------------------------------------------------------