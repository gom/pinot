package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertConfigBean extends AbstractBean {
  String name;
  String cronExpression;
  boolean active;
  EmailConfig emailConfig;
  ReportConfig reportConfig;

  public String getCronExpression() {
    return cronExpression;
  }

  public void setCronExpression(String cronExpression) {
    this.cronExpression = cronExpression;
  }

  public EmailConfig getEmailConfig() {
    return emailConfig;
  }

  public void setEmailConfig(EmailConfig emailConfig) {
    this.emailConfig = emailConfig;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ReportConfig getReportConfig() {
    return reportConfig;
  }

  public void setReportConfig(ReportConfig reportConfig) {
    this.reportConfig = reportConfig;
  }

  public static class EmailConfig {
    boolean sendAlertOnZeroAnomaly;
    long lastNotifiedAnomalyId;
    List<Long> functionIds;

    public List<Long> getFunctionIds() {
      return functionIds;
    }

    public void setFunctionIds(List<Long> functionIds) {
      this.functionIds = functionIds;
    }

    public long getLastNotifiedAnomalyId() {
      return lastNotifiedAnomalyId;
    }

    public void setLastNotifiedAnomalyId(long lastNotifiedAnomalyId) {
      this.lastNotifiedAnomalyId = lastNotifiedAnomalyId;
    }

    public boolean isSendAlertOnZeroAnomaly() {
      return sendAlertOnZeroAnomaly;
    }

    public void setSendAlertOnZeroAnomaly(boolean sendAlertOnZeroAnomaly) {
      this.sendAlertOnZeroAnomaly = sendAlertOnZeroAnomaly;
    }
  }

  public static class ReportConfig {
    boolean enabled = true;
    List<Long> metricIds;
    List<List<String>> metricDimensions;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public List<List<String>> getMetricDimensions() {
      return metricDimensions;
    }

    public void setMetricDimensions(List<List<String>> metricDimensions) {
      this.metricDimensions = metricDimensions;
    }

    public List<Long> getMetricIds() {
      return metricIds;
    }

    public void setMetricIds(List<Long> metricIds) {
      this.metricIds = metricIds;
    }
  }
}
