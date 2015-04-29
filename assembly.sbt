import AssemblyKeys._ // put this at the top of the file

assemblySettings

// your assembly settings here
// Do not include the scala library
// assemblyOption in assembly ~= { _.copy(includeScala = false) }

// Easier name
jarName in assembly := "project.jar"