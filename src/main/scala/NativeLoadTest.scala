object NativeLoadTest {
  def main(args: Array[String]): Unit = {
    val dll = "C:\\hadoop\\bin\\hadoop.dll"
    println("HADOOP_HOME(env): " + sys.env.getOrElse("HADOOP_HOME","<no>"))
    println("PATH(env) has C:\\hadoop\\bin? " + sys.env.getOrElse("PATH","").toLowerCase.contains("c:\\hadoop\\bin"))
    println("java.library.path: " + System.getProperty("java.library.path"))
    System.load(dll)
    println(sys.env.getOrElse("PATH",""))
    println("OK: hadoop.dll loaded")
    println("Native loaded? " + org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
  }
}
