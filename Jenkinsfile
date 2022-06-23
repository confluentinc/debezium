#!/usr/bin/env groovy
/*
 * Copyright [2019 - 2020] Confluent Inc.
 */
def config = jobConfig {}

common {
  slackChannel = '#connect-warn'
  nodeLabel = 'docker-openjdk11'
  downStreamValidate = false
  timeoutHours = 3
  mavenProfiles = 'oracle,oracle-ci,-xstream-dependency'
}