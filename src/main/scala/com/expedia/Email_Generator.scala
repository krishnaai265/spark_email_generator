package com.expedia

import org.apache.spark.sql.{DataFrame}

import javax.mail._
import javax.mail.internet._

class Email_Generator {

  var prepareHTML = new Prepare_HTML();

  def Display(df: DataFrame) {

    val username = "krishnaai265@gmail.com"
    val password = ""
    val smtpHost = "smtp.gmail.com"

    // Set up the mail object
    val properties = System.getProperties
    properties.put("mail.smtp.host", smtpHost)
    properties.put("mail.smtp.user", username);
    properties.put("mail.smtp.password", password);
    properties.put("mail.smtp.auth", "true");
    properties.put("mail.smtp.port", "587")
    properties.put("mail.smtp.starttls.enable", "true");

    val auth:Authenticator = new Authenticator() {
      override def getPasswordAuthentication = new
          PasswordAuthentication(username, password)
    }

    val session = Session.getInstance(properties,auth)
    val message = new MimeMessage(session)

    // Set the from, to, subject, body text
    message.setFrom(new InternetAddress("krishnaai265@gmail.com"))
    message.setRecipients(Message.RecipientType.TO, "rajisingh@expediagroup.com")//"rajisingh@expediagroup.com"
    message.setHeader("Content-Type", "text/html")
    message.setSubject("Email Generator")
    message.setContent(prepareHTML.getHTML(df), "text/html")

    Transport.send(message)

  }
/*
  def getHTML(df: DataFrame): String = {
    var emailContent = "<table>"
    df.collect().foreach(row => {
      emailContent +="<tr>"
      row.toSeq.foreach(col => emailContent += "<td>" + col + "</td>")
      emailContent +="</tr>"
    }
    )
    emailContent+="</table>"
    emailContent
  }
 */

}
