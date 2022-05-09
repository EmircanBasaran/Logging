from module import Module as md
import psycopg2
import smtplib, ssl, sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
import datetime

def etlmail(ETLDATE):

    sender_email = "sender_mail"
    receiver_email = "receiver_mail"
    password = 'sender_password'

    message = MIMEMultipart("ETL")
    message["Subject"] = "RAW ETL SUMMARY"
    message["From"] = sender_email
    message["To"] = receiver_email


    #RAW LOGS
    SQL="select b.companyname,a.companyid,a.msystemid,FUNCTIONNAME,ERR,JOBDATE FROM RAW.LOGS A left join consolide.company b on a.companyid=b.companyid WHERE JOBDATE>'{}' AND ISSUCCESS=0".format(ETLDATE)
    cursor.execute(SQL)
    data2 = cursor.fetchall()
    color='red'

    html = """
                            </table>
                            </div>
                            <p></p>
                            <p></p>
                            <div class="container">
                            <h3>RAW FAILURE CONTROL</h3>
                            <table border='1' bordercolor='gainsboro'>
                            <tr class="info" bgcolor={}>
                            <th>COMPANYNAME</th>
                            <th>COMPANYID</th>
                            <th>MSYSTEMID</th>
                            <th>FUNCTIONNAME</th>
                            <th>ERROR</th>
                            <th>DATE</th>
                            </tr>
                            """.format(color)

    for i in range(len(data2)):
        html += """
                                   <td bgcolor={} >{}</td>
                                   <td bgcolor={} >{}</td>
                                   <td bgcolor={} >{}</td>
                                   <td bgcolor={} >{}</td>
                                   <td bgcolor={} >{}</td>
                                   <td bgcolor={} >{}</td>
                                   </tr>
                                   """.format(color, data2[i][0], color, data2[i][1], color, data2[i][2], color, data2[i][3], color, data2[i][4], color, data2[i][5])
    #JOBARCHIVE
    SQL2 = "SELECT (select companyname from consolide.company d where a.companyid=d.companyid)," \
           "A.COMPANYID,A.MSYSTEMID,(SELECT COUNT(*) FROM raw.jobarchive B WHERE A.MSYSTEMID=B.MSYSTEMID AND A.COMPANYID=B.COMPANYID and ETLDATE>'{}' GROUP BY COMPANYID,MSYSTEMID) AS NEWCOUNT,A.TOTALCOUNT,A.JOBDATE,A.ETLDATE," \
           "(select MAX(jobdate) from raw.logs C where A.MSYSTEMID=C.MSYSTEMID and A.COMPANYID=C.COMPANYID) as LOGDATE FROM (select COMPANYID,MSYSTEMID,COUNT(*) as TOTALCOUNT,max(JOBDATE) as JOBDATE,max(ETLDATE) as ETLDATE  from raw.jobarchive group by COMPANYID,MSYSTEMID ORDER BY COMPANYID,MSYSTEMID) A".format(ETLDATE)
    cursor.execute(SQL2)
    data = cursor.fetchall()
    color = 'lightblue'
    html += """
                            </table>
                            </div>
                            <p></p>
                            <p></p>
                            <div class="container">
                            <h3>JOBARCHIVE COUNT</h3>
                            <table border='1' bordercolor='gainsboro'>
                            <tr class="info" bgcolor={}>
                            <th>COMPANYNAME</th>
                            <th>COMPANYID</th>
                            <th>MSYSTEMID</th>
                            <th>NEW</th>
                            <th>TOTAL</th>
                            <th>MAX(JOBDATE)</th>
                            <th>MAX(ETLDATE)</th>
                            <th>MAX(LOGDATE)</th>
                            </tr>
                            """.format(color)

    for i in range(len(data)):
        if data[i][3] == None:
            color = 'red'
        else:
            color = 'lightgreen'
        html += """
            <td  bgcolor={} >{}</td>
            <td  bgcolor={} >{}</td>
            <td  bgcolor={} >{}</td>
            <td  bgcolor={} >{}</td>
            <td  bgcolor={} >{}</td>
            <td  bgcolor={} >{}</td>
            <td  bgcolor={} >{}</td>
            <td  bgcolor={} >{}</td>
            </tr>
            """.format(color, data[i][0], color, data[i][1], color, data[i][2], color, data[i][3], color, data[i][4], color, data[i][5], color, data[i][6], color, data[i][7])

    #JOBS
    SQL8 = "SELECT (select companyname from consolide.company d where a.companyid=d.companyid)," \
           "A.COMPANYID,A.MSYSTEMID,(SELECT COUNT(*) FROM RAW.JOBS B WHERE A.MSYSTEMID=B.MSYSTEMID AND A.COMPANYID=B.COMPANYID and ETLDATE>'{}' GROUP BY COMPANYID,MSYSTEMID) AS NEWCOUNT,A.TOTALCOUNT,A.JOBDATE,A.ETLDATE, " \
           "(select MAX(jobdate) from raw.logs C where A.MSYSTEMID=C.MSYSTEMID and A.COMPANYID=C.COMPANYID) as LOGDATE FROM (select COMPANYID,MSYSTEMID,COUNT(*) as TOTALCOUNT,MAX(DATE) as JOBDATE,MAX(ETLDATE) as ETLDATE from RAW.JOBS group by COMPANYID,MSYSTEMID ORDER BY COMPANYID,MSYSTEMID) A".format(ETLDATE)
    cursor.execute(SQL8)
    data = cursor.fetchall()
    color = 'lightblue'
    html += """
                                            </table>
                                            </div>
                                            <p></p>
                                            <p></p>
                                            <div class="container">
                                            <h3>JOBS COUNT</h3>
                                            <table border='1' bordercolor='gainsboro'>
                                            <tr class="info" bgcolor={}>
                                            <th>COMPANYNAME</th>
                                            <th>COMPANYID</th>
                                            <th>MSYSTEMID</th>
                                            <th>NEW</th>
                                            <th>TOTAL</th>
                                            <th>MAX(JOBDATE)</th>
                                            <th>MAX(ETLDATE)</th>
                                            <th>MAX(LOGDATE)</th>
                                            </tr>
                                            """.format(color)

    for i in range(len(data)):
        if data[i][3] == None:
            color = 'red'
        else:
            color = 'lightgreen'
        html += """
                            <td  bgcolor={} >{}</td>
                            <td  bgcolor={} >{}</td>
                            <td  bgcolor={} >{}</td>
                            <td  bgcolor={} >{}</td>
                            <td  bgcolor={} >{}</td>
                            <td  bgcolor={} >{}</td>
                            <td  bgcolor={} >{}</td>
                            <td  bgcolor={} >{}</td>
                            </tr>
                            """.format(color, data[i][0], color, data[i][1], color, data[i][2], color, data[i][3], color, data[i][4], color, data[i][5], color, data[i][6], color, data[i][7])
    #JOBSDETAIL
    SQL8 = "SELECT A.COMPANYID,A.MSYSTEMID,(SELECT COUNT(*) FROM RAW.JOBS_DETAIL B WHERE A.MSYSTEMID=B.MSYSTEMID AND A.COMPANYID=B.COMPANYID and ETLDATE>'{}' GROUP BY COMPANYID,MSYSTEMID) AS NEWCOUNT,A.TOTALCOUNT FROM (select COMPANYID,MSYSTEMID,COUNT(*) as TOTALCOUNT from RAW.JOBS_DETAIL group by COMPANYID,MSYSTEMID ORDER BY COMPANYID,MSYSTEMID) A".format(ETLDATE)
    cursor.execute(SQL8)
    data = cursor.fetchall()
    color = 'lightblue'
    html += """
                                                </table>
                                                </div>
                                                <p></p>
                                                <p></p>
                                                <div class="container">
                                                <h3>JOBSDETAIL COUNT</h3>
                                                <table border='1' bordercolor='gainsboro'>
                                                <tr class="info" bgcolor={}>
                                                <th>COMPANYID</th>
                                                <th>MSYSTEMID</th>
                                                <th>NEW</th>
                                                <th>TOTAL</th>
                                                </tr>
                                                """.format(color)

    for i in range(len(data)):
        if data[i][2] == None:
            color = 'red'
        else:
            color = 'lightgreen'
        html += """
                                <td  bgcolor={} >{}</td>
                                <td  bgcolor={} >{}</td>
                                <td  bgcolor={} >{}</td>
                                <td  bgcolor={} >{}</td>
                                </tr>
                                """.format(color, data[i][0], color, data[i][1], color, data[i][2], color, data[i][3])

    #ALARMS
    SQL3 = "SELECT A.COMPANYID,A.MSYSTEMID,(SELECT COUNT(*) FROM raw.alarms B WHERE A.MSYSTEMID=B.MSYSTEMID AND A.COMPANYID=B.COMPANYID and ETLDATE>'{}' GROUP BY COMPANYID,MSYSTEMID) AS NEWCOUNT,A.TOTALCOUNT FROM (select COMPANYID,MSYSTEMID,COUNT(*) as TOTALCOUNT from raw.alarms group by COMPANYID,MSYSTEMID ORDER BY COMPANYID,MSYSTEMID) A".format(ETLDATE)
    cursor.execute(SQL3)
    data = cursor.fetchall()
    color = 'lightblue'
    html += """
                                </table>
                                </div>
                                <p></p>
                                <p></p>
                                <div class="container">
                                <h3>ALARMS COUNT</h3>
                                <table border='1' bordercolor='gainsboro'>
                                <tr class="info" bgcolor={}>
                                <th>COMPANYID</th>
                                <th>MSYSTEMID</th>
                                <th>NEW</th>
                                <th>TOTAL</th>
                                </tr>
                                """.format(color)

    for i in range(len(data)):
        if data[i][2] == None:
            color = 'red'
        else:
            color = 'lightgreen'
        html += """
                <td  bgcolor={} >{}</td>
                <td  bgcolor={} >{}</td>
                <td  bgcolor={} >{}</td>
                <td  bgcolor={} >{}</td>
                </tr>
                """.format(color, data[i][0], color, data[i][1], color, data[i][2], color, data[i][3])
    #EVENTS
    SQL4 = "SELECT A.COMPANYID,A.MSYSTEMID,(SELECT COUNT(*) FROM RAW.EVENT B WHERE A.MSYSTEMID=B.MSYSTEMID AND A.COMPANYID=B.COMPANYID and ETLDATE>'{}' GROUP BY COMPANYID,MSYSTEMID) AS NEWCOUNT,A.TOTALCOUNT FROM (select COMPANYID,MSYSTEMID,COUNT(*) as TOTALCOUNT from RAW.EVENT group by COMPANYID,MSYSTEMID ORDER BY COMPANYID,MSYSTEMID) A".format(ETLDATE)
    cursor.execute(SQL4)
    data = cursor.fetchall()
    color = 'lightblue'
    html += """
                                    </table>
                                    </div>
                                    <p></p>
                                    <p></p>
                                    <div class="container">
                                    <h3>EVENT COUNT</h3>
                                    <table border='1' bordercolor='gainsboro'>
                                    <tr class="info" bgcolor={}>
                                    <th>COMPANYID</th>
                                    <th>MSYSTEMID</th>
                                    <th>NEW</th>
                                    <th>TOTAL</th>
                                    </tr>
                                    """.format(color)

    for i in range(len(data)):
        if data[i][2] == None:
            color = 'red'
        else:
            color = 'lightgreen'
        html += """
                    <td  bgcolor={} >{}</td>
                    <td  bgcolor={} >{}</td>
                    <td  bgcolor={} >{}</td>
                    <td  bgcolor={} >{}</td>
                    </tr>
                    """.format(color, data[i][0], color, data[i][1], color, data[i][2], color, data[i][3])
    #CHEMPARAMS
    SQL5 = "SELECT A.COMPANYID,A.MSYSTEMID,(SELECT COUNT(*) FROM RAW.CHEMPARAMS B WHERE A.MSYSTEMID=B.MSYSTEMID AND A.COMPANYID=B.COMPANYID and ETLDATE>'{}' GROUP BY COMPANYID,MSYSTEMID) AS NEWCOUNT,A.TOTALCOUNT FROM (select COMPANYID,MSYSTEMID,COUNT(*) as TOTALCOUNT from RAW.CHEMPARAMS group by COMPANYID,MSYSTEMID ORDER BY COMPANYID,MSYSTEMID) A".format(ETLDATE)
    cursor.execute(SQL5)
    data = cursor.fetchall()
    color = 'lightblue'
    html += """
                                    </table>
                                    </div>
                                    <p></p>
                                    <p></p>
                                    <div class="container">
                                    <h3>CHEMPARAMS COUNT</h3>
                                    <table border='1' bordercolor='gainsboro'>
                                    <tr class="info" bgcolor={}>
                                    <th>COMPANYID</th>
                                    <th>MSYSTEMID</th>
                                    <th>NEW</th>
                                    <th>TOTAL</th>
                                    </tr>
                                    """.format(color)

    for i in range(len(data)):
        if data[i][2] == None:
            color = 'red'
        else:
            color = 'lightgreen'
        html += """
                    <td  bgcolor={} >{}</td>
                    <td  bgcolor={} >{}</td>
                    <td  bgcolor={} >{}</td>
                    <td  bgcolor={} >{}</td>
                    </tr>
                    """.format(color, data[i][0], color, data[i][1], color, data[i][2], color, data[i][3])
    #MACHPARAMS
    SQL6 = "SELECT A.COMPANYID,A.MSYSTEMID,(SELECT COUNT(*) FROM RAW.MACHPARAMS B WHERE A.MSYSTEMID=B.MSYSTEMID AND A.COMPANYID=B.COMPANYID and ETLDATE>'{}' GROUP BY COMPANYID,MSYSTEMID) AS NEWCOUNT,A.TOTALCOUNT FROM (select COMPANYID,MSYSTEMID,COUNT(*) as TOTALCOUNT from RAW.MACHPARAMS group by COMPANYID,MSYSTEMID ORDER BY COMPANYID,MSYSTEMID) A".format(ETLDATE)
    cursor.execute(SQL6)
    data = cursor.fetchall()
    color = 'lightblue'
    html += """
                                        </table>
                                        </div>
                                        <p></p>
                                        <p></p>
                                        <div class="container">
                                        <h3>MACHPARAMS COUNT</h3>
                                        <table border='1' bordercolor='gainsboro'>
                                        <tr class="info" bgcolor={}>
                                        <th>COMPANYID</th>
                                        <th>MSYSTEMID</th>
                                        <th>NEW</th>
                                        <th>TOTAL</th>
                                        </tr>
                                        """.format(color)

    for i in range(len(data)):
        if data[i][2] == None:
            color = 'red'
        else:
            color = 'lightgreen'
        html += """
                        <td  bgcolor={} >{}</td>
                        <td  bgcolor={} >{}</td>
                        <td  bgcolor={} >{}</td>
                        <td  bgcolor={} >{}</td>
                        </tr>
                        """.format(color, data[i][0], color, data[i][1], color, data[i][2], color, data[i][3])
    #JOBARCHIVECHEMS
    SQL7 = "SELECT A.COMPANYID,A.MSYSTEMID,(SELECT COUNT(*) FROM RAW.JOBARCHIVECHEMS B WHERE A.MSYSTEMID=B.MSYSTEMID AND A.COMPANYID=B.COMPANYID and ETLDATE>'{}' GROUP BY COMPANYID,MSYSTEMID) AS NEWCOUNT,A.TOTALCOUNT FROM (select COMPANYID,MSYSTEMID,COUNT(*) as TOTALCOUNT from RAW.JOBARCHIVECHEMS group by COMPANYID,MSYSTEMID ORDER BY COMPANYID,MSYSTEMID) A".format(ETLDATE)
    cursor.execute(SQL7)
    data = cursor.fetchall()
    color = 'lightblue'
    html += """
                                            </table>
                                            </div>
                                            <p></p>
                                            <p></p>
                                            <div class="container">
                                            <h3>JOBARCHIVECHEMS COUNT</h3>
                                            <table border='1' bordercolor='gainsboro'>
                                            <tr class="info" bgcolor={}>
                                            <th>COMPANYID</th>
                                            <th>MSYSTEMID</th>
                                            <th>NEW</th>
                                            <th>TOTAL</th>
                                            </tr>
                                            """.format(color)

    for i in range(len(data)):
        if data[i][2] == None:
            color = 'red'
        else:
            color = 'lightgreen'
        html += """
                            <td  bgcolor={} >{}</td>
                            <td  bgcolor={} >{}</td>
                            <td  bgcolor={} >{}</td>
                            <td  bgcolor={} >{}</td>
                            </tr>
                            """.format(color, data[i][0], color, data[i][1], color, data[i][2], color, data[i][3])


    # part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")
    # message.attach(part1)
    message.attach(part2)

    # Create secure connection with server and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string()
        )
    print('sendMail basariyla calisti')


if __name__=='__main__':
    cursor, conn = md.connect(COMPANYID=-99, MSYSTEMID=-99)
    md.log(-99, -99, "'etlmailRawSTART'", 1, 'Success')
    try:
        etlmail(sys.argv[1])
        md.log(-99, -99, "'etlmailRaw'", 1, 'Success')
    except Exception as Err:
        print(Exception)
        md.log(-99, -99, "'etlmailRaw'", 0, Err)
    md.log(-99, -99, "'etlmailRawEND'", 1, 'Success')
    md.connectEnd(COMPANYID=-99, MSYSTEMID=-99)
