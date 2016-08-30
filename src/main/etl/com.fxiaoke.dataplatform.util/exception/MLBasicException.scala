package com.fxiaoke.dataplatform.util.exception

/**
  * Created by wujing on 2016/7/16.
  */
class MLBasicException(message: String, t: Throwable) extends RuntimeException(message, t) {
  def this(message: String) = this(message, null)

  def this(t: Throwable) = this("", t)
}
