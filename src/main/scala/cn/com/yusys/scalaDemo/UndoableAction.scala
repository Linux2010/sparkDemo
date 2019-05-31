package cn.com.yusys.scalaDemo

abstract class UndoableAction {
  def undo():Unit
  def redo():Unit
}
