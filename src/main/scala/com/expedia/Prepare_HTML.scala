package com.expedia

import org.apache.spark.sql.DataFrame

class Prepare_HTML {
  def getHTML(df: DataFrame): String = {
    var emailContent =
      """<!DOCTYPE html>
      <html>
        <head>
          <style>
            table, td, th {
            border: 1px solid #ddd;
            text-align: left;
            font-family:"source-sans-pro",helvetica;
    color: #333;
    }

    table {
    border-collapse: collapse;
    width: 100%;
    }

    th, td {
    padding: 15px;
    }

    th {
    background-color: #f0f0f0;
    text-align: center;
    }
    td {
    text-align: right;
    }
    #color-red {
    background-color: #ffcccc;
    }
    #color-green {
    background-color: #ccffcc;
    }

    </style>
    </head>
    <body>

      <p>Please verify below dataframe:</p>

      <table><tr>"""

    df.columns.foreach(row => {
      emailContent += "<th>" +row +"</th>"
    })
    emailContent+="</tr>"

    var itr =0
    val resultRow = df.count()
    val resultSet = df.collectAsList
    while ( itr < resultRow ){
      val col1 = resultSet.get(itr).getString(0)
      val col2 = "%,d".format(resultSet.get(itr).getInt(1))
      val col3 = resultSet.get(itr).getDouble(2)
      val col4 = "%,d".format(resultSet.get(itr).getInt(1))
      val col5 = resultSet.get(itr).getDouble(4)
      val col6 = "%,d".format(resultSet.get(itr).getInt(1))
      val col7 = resultSet.get(itr).getDouble(6)
      val col8 = "%,d".format(resultSet.get(itr).getInt(1))
      val col9 = resultSet.get(itr).getDouble(8)

      emailContent +="<tr>"
      emailContent +="<td>"+col1+"</td>"
      emailContent +="<td>"+col2+"</td>"
      emailContent +=colorChange(col3)
      emailContent +="<td>"+col4+"</td>"
      emailContent +=colorChange(col5)
      emailContent +="<td>"+col6+"</td>"
      emailContent +=colorChange(col7)
      emailContent +="<td>"+col8+"</td>"
      emailContent +=colorChange(col9)
      emailContent+="</tr>"
      itr = itr + 1
    }
    emailContent += """</table>

    <p>Thank You,</p>
    <p>Krishna Kumar Singh</p>

    </body>
    </html>"""

    emailContent
  }

  def colorChange(value: Double): String ={
    if(value > -0.10 && value <=0.10){
      return "<td id='color-green'>"+value+"</td>"
    }
    else{
      return "<td id='color-red'>"+value+"</td>"
    }
  }

}
