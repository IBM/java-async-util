//
// (C) Copyright IBM Corp. 2005 All Rights Reserved.
//
// Contact Information:
//
// IBM Corporation
// Legal Department
// 222 South Riverside Plaza
// Suite 1700
// Chicago, IL 60606, USA
//
// END-OF-HEADER
//
// -----------------------
// @author: renar
//
// Date: Aug 26, 2015
// ---------------------

package com.ibm.async_util.util;

import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface AsyncCloseable {
  CompletionStage<Void> close();
}
