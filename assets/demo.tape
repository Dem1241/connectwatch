# records assets/demo.gif from the --demo cluster: vhs assets/demo.tape
Output assets/demo.gif

Set Shell bash
Set FontSize 16
Set Width 1400
Set Height 760
Set Padding 20
Set Theme "Catppuccin Mocha"

Type "connectwatch --demo"
Sleep 500ms
Enter
Sleep 4s

# the trace and the config of the failed debezium task
Tab
Sleep 3s
Tab
Sleep 2.5s
Tab
Sleep 1.5s

# restarting it doesn't help, the record is still too large
Type "R"
Sleep 1.5s
Type "y"
Sleep 14s

# the s3 sink only hit one bad record, so a restart does
Type "/s3"
Sleep 1s
Enter
Sleep 1s
Type "R"
Sleep 1.5s
Type "y"
Sleep 3s
Escape
Sleep 4s

Type "q"
