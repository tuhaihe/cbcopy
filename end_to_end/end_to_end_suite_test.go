package end_to_end_test

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/operating"

	//"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/cloudberrydb/cbcopy/internal/dbconn"

	//"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/cloudberrydb/cbcopy/internal/testhelper"

	//"github.com/greenplum-db/gpbackup/backup"
	"github.com/cloudberrydb/cbcopy/meta/builtin"

	// "github.com/greenplum-db/gpbackup/filepath"

	//"github.com/greenplum-db/gpbackup/testutils"
	"github.com/cloudberrydb/cbcopy/testutils"

	"github.com/spf13/pflag"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	. "github.com/onsi/gomega/gexec"
)

/*
readme

- ERROR: command error message: sh: cbcopy_helper: command not found (SQLSTATE 2F000)
- table space directory
*/

type instance struct {
	host string
	port int
}

var (
	env = map[string]instance{
		"cbdb": {"192.168.178.50", 5432},
		"3x":   {"192.168.179.47", 5432},
		"gp6":  {"192.168.176.201", 5432},
		"gp5":  {"192.168.177.8", 5432},
		"gp4":  {"192.168.176.7", 5432},
		"1x":   {"192.168.177.168", 5432},
		"gp7":  {"192.168.176.17", 5432},
	}

	//
	versionSource = "cbdb"
	versionTarget = "cbdb"
	//versionSource = "3x"
	//versionTarget = "3x"

	// env information
	sourceHost = env[versionSource].host
	sourcePort = env[versionSource].port
	sourceUser = "gpadmin"
	sourceDb   = "db1"

	targetHost = env[versionTarget].host
	targetPort = env[versionTarget].port
	targetUser = "gpadmin"
	targetDb   = "db1_copy"

	psqlVersion, sourceVersion, targetVersion string

	// connection
	sourceConn *dbconn.DBConn

	// binary path
	cbcopyPath string

	// if a test case match both, priority order: userRequestSkipCaseList first
	userRequestSkipCaseList []string
	userRequestRunCaseList  []string

	runCaseList     []string
	runPassCaseList []string
	runFailCaseList []string
	runSkipCaseList []string

	context_currentCaseId string
)

func TestEndToEnd(t *testing.T) {
	format.MaxLength = 0
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndToEnd Suite")
}

func dbVersionValidCheck(dbVersion string) {
	switch dbVersion {
	case "cbdb":
	case "3x":
	case "gp6":
	case "gp5":
	case "gp4":
	case "1x":
		return
	default:
		msg := fmt.Sprintf("    -- Invalid db version input: %s, valid value: %s \n", dbVersion, "[cbdb, 3x, gp6, gp5, gp4, 1x]")
		fmt.Printf("%v \n", msg)
		Fail(msg)
	}
}

var _ = BeforeSuite(func() {
	fmt.Println("")
	fmt.Println("--> BeforeSuite()")

	testhelper.SetupTestLogger()

	if os.Getenv("HASHCOPY_TEST_SOURCE_VERSION") != "" {
		versionSource = os.Getenv("HASHCOPY_TEST_SOURCE_VERSION")
	}
	if os.Getenv("HASHCOPY_TEST_TARGET_VERSION") != "" {
		versionTarget = os.Getenv("HASHCOPY_TEST_TARGET_VERSION")
	}
	dbVersionValidCheck(versionSource)
	dbVersionValidCheck(versionTarget)

	if os.Getenv("HASHCOPY_TEST_SKIP_CASE_LIST") != "" {
		userRequestSkipCaseList = strings.Split(strings.TrimSpace(os.Getenv("HASHCOPY_TEST_SKIP_CASE_LIST")), ",")
	}
	if os.Getenv("HASHCOPY_TEST_RUN_CASE_LIST") != "" {
		userRequestRunCaseList = strings.Split(strings.TrimSpace(os.Getenv("HASHCOPY_TEST_RUN_CASE_LIST")), ",")
	}

	// env information
	sourceHost = env[versionSource].host
	sourcePort = env[versionSource].port

	targetHost = env[versionTarget].host
	targetPort = env[versionTarget].port

	psqlVersion = testutils.GetPslVersion()
	sourceVersion = testutils.GetServerVersion(sourceHost, sourcePort, sourceUser)
	targetVersion = testutils.GetServerVersion(targetHost, targetPort, targetUser)

	fmt.Printf("    -- Test Environment\n")
	fmt.Printf("    -- Env variable checking: HASHCOPY_TEST_SOURCE_VERSION, HASHCOPY_TEST_TARGET_VERSION, HASHCOPY_TEST_SKIP_CASE_LIST\n")
	fmt.Printf("    -- Env: %v ---> %v,  userRequestSkipCaseList: %v, userRequestRunCaseList: %v, \n", versionSource, versionTarget, userRequestSkipCaseList, userRequestRunCaseList)
	fmt.Printf("    -- psqlVersion: %v, \n       sourceVersion: %v, \n       targetVersion: %v \n\n", psqlVersion, sourceVersion, targetVersion)

	runCommand("dropdb", fmt.Sprintf("--host=%s", sourceHost), fmt.Sprintf("--port=%d", sourcePort), sourceDb)
	runCommand("dropdb", fmt.Sprintf("--host=%s", targetHost), fmt.Sprintf("--port=%d", targetPort), targetDb)

	runCommand("createdb", fmt.Sprintf("--host=%s", sourceHost), fmt.Sprintf("--port=%d", sourcePort), sourceDb)
	//runCommand("createdb", fmt.Sprintf("--host=%s", targetHost), fmt.Sprintf("--port=%d", targetPort), targetDb)

	sourceConn = dbconn.NewDBConn(sourceDb, sourceUser, sourceHost, sourcePort)

	backupCmdFlags := pflag.NewFlagSet("gpbackup", pflag.ExitOnError)
	builtin.SetCmdFlags(backupCmdFlags)
	builtin.InitializeMetadataParams(sourceConn)
	builtin.SetFilterRelationClause("")

	binDir := fmt.Sprintf("%s/go/bin", operating.System.Getenv("HOME"))
	if os.Getenv("GOPATH") != "" {
		binDir = fmt.Sprintf("%s/bin", operating.System.Getenv("GOPATH"))
	}

	cbcopyPath = fmt.Sprintf("%s/cbcopy", binDir)

	fmt.Println("<-- BeforeSuite()")
})

var _ = AfterSuite(func() {
	fmt.Println("")
	fmt.Println("--> AfterSuite()")

	fmt.Println("")
	fmt.Printf("    ## Case Run Result Summary ## \n")
	fmt.Printf("    -- runCaseList: total (%v), %v \n", len(runCaseList), runCaseList)
	fmt.Printf("    -- runPassCaseList: total (%v), %v \n", len(runPassCaseList), runPassCaseList)
	fmt.Printf("    -- runFailCaseList: total (%v), %v \n", len(runFailCaseList), runFailCaseList)
	fmt.Printf("    -- runSkipCaseList: total (%v), %v \n", len(runSkipCaseList), runSkipCaseList)

	// print env info again
	fmt.Println("")
	fmt.Printf("    ## Case Run Environment Summary ## \n")
	fmt.Printf("    -- Env: %v ---> %v,  userRequestSkipCaseList: %v, userRequestRunCaseList: %v \n", versionSource, versionTarget, userRequestSkipCaseList, userRequestRunCaseList)
	fmt.Printf("    -- psqlVersion: %v, \n       sourceVersion: %v, \n       targetVersion: %v \n\n", psqlVersion, sourceVersion, targetVersion)

	if sourceConn != nil {
		sourceConn.Close()
	}

	CleanupBuildArtifacts()

	fmt.Println("<-- AfterSuite()")
})

func checkResult(targetHost string, targetPort int, targetUser string, targetDb string, sqlCheckFile string, resultFile string, expectResultFile string) {
	testutils.ExecuteSQLFileV2WithSaveResultFile(targetHost, targetPort, targetUser, targetDb, sqlCheckFile, resultFile)
	compareFile(resultFile, expectResultFile)
}

func compareFile(resultFileName string, expectResultFileName string) {
	fmt.Printf("    -- File compare, %s %s\n", resultFileName, expectResultFileName)

	fileResultFile, err := os.Open(resultFileName)
	if err != nil {
		fmt.Printf("    -- File compare, %s %s, result: FAIL\n", resultFileName, expectResultFileName)

		msg := fmt.Sprintf("Failed to open: %s. Error: %s", resultFileName, err.Error())
		fmt.Printf("%v \n", msg)
		Fail(msg)
	}
	defer fileResultFile.Close()

	fileExpectResultFile, err := os.Open(expectResultFileName)
	if err != nil {
		fmt.Printf("    -- File compare, %s %s, result: FAIL\n", resultFileName, expectResultFileName)

		msg := fmt.Sprintf("Failed to open: %s. Error: %s", expectResultFileName, err.Error())
		fmt.Printf("%v \n", msg)
		Fail(msg)
	}
	defer fileExpectResultFile.Close()

	scannerResultFile := bufio.NewScanner(fileResultFile)
	scannerExpectResultFile := bufio.NewScanner(fileExpectResultFile)

	for scannerExpectResultFile.Scan() && scannerResultFile.Scan() {
		if scannerExpectResultFile.Text() != scannerResultFile.Text() {
			fmt.Printf("    -- File compare, %s %s, result: DIFF\n", resultFileName, expectResultFileName)

			msg := fmt.Sprintf("    -- File compare, %s %s, result: DIFFERENT\n got: (%s), \n expect: (%s)\n",
				resultFileName, expectResultFileName,
				scannerResultFile.Text(), scannerExpectResultFile.Text())
			fmt.Printf("%v \n", msg)
			Fail(msg)
		}
	}

	if scannerExpectResultFile.Scan() || scannerResultFile.Scan() {
		fmt.Printf("    -- File compare, %s %s, result: DIFF\n", resultFileName, expectResultFileName)

		msg := fmt.Sprintf("    -- File compare, %s %s, result: DIFFERENT\n got: (%s), \n expect: (%s)\n",
			resultFileName, expectResultFileName,
			scannerResultFile.Text(), scannerExpectResultFile.Text())
		fmt.Printf("%v \n", msg)
		Fail(msg)
	}

	fmt.Printf("    -- File compare, %s %s, result: SAME\n", resultFileName, expectResultFileName)
}

func cbcopy(args ...string) {
	args = append([]string{"--verbose"}, args...)
	runCommand(cbcopyPath, args...)
}

func runCommand(cmd string, args ...string) {
	fmt.Printf("    -- runCommand, %v %v\n", cmd, args)
	command := exec.Command(cmd, args...)
	output, err := command.CombinedOutput()
	if err != nil && !strings.Contains(err.Error(), "not exist") && !strings.Contains(string(output), "not exist") {
		fmt.Printf("%s", output)
		Fail(fmt.Sprintf("%v", err))
	}
}

func cleanupObjects(dropScriptFileName, checkScriptFileName, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget string) {
	fmt.Println("")
	fmt.Println("    -- cleanup objects in source and target db --")

	// let's do the drop first, as currently checkResult would jump out if it does not see expect result
	testutils.ExecuteSQLFileV2(sourceHost, sourcePort, sourceUser, sourceDb, dropScriptFileName)
	testutils.ExecuteSQLFileV2(targetHost, targetPort, targetUser, targetDb, dropScriptFileName)

	checkResult(sourceHost, sourcePort, sourceUser, sourceDb,
		checkScriptFileName,
		checkDropResultFileName,
		checkDropResultFileNameExpectSource)

	checkResult(targetHost, targetPort, targetUser, targetDb,
		checkScriptFileName,
		checkDropResultFileName,
		checkDropResultFileNameExpectTarget)
}

func initFileNames(objectType string, scenario string) (string, string, string, string, string, string, string, string, string, string) {
	createScriptFileName := fmt.Sprintf("resources/%s.create.sql", objectType)
	checkScriptFileName := fmt.Sprintf("resources/%s.check.sql", objectType)
	dropScriptFileName := fmt.Sprintf("resources/%s.drop.sql", objectType)

	checkCreateResultFileName := fmt.Sprintf("%s.create.check.output", objectType)
	checkCreateResultFileNameExpect := fmt.Sprintf("resources/%s.create.check.output.expect", objectType)

	checkDropResultFileName := fmt.Sprintf("%s.drop.check.output", objectType)
	checkDropResultFileNameExpectSource := fmt.Sprintf("resources/%s.drop.check.output.expect", objectType)
	checkDropResultFileNameExpectTarget := fmt.Sprintf("resources/%s.drop.check.output.expect", objectType)

	checkCbcopyResultFileName := fmt.Sprintf("%s.cbcopy.check.output", objectType)
	checkCbcopyResultFileNameExpect := fmt.Sprintf("resources/%s.cbcopy.check.output%s.expect", objectType, scenario)

	switch objectType {
	case "tables_objects":
		if versionSource == "gp5" || versionSource == "gp4" || versionSource == "1x" {
			checkDropResultFileNameExpectSource = checkDropResultFileNameExpectSource + ".gp5OrPre"
		}
		break
	}

	return createScriptFileName, checkScriptFileName, dropScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect
}

func getFileName(objectType string, requestType string, scenario string) string {
	if requestType == "checkCbcopyResultFileNameExpect" {
		checkCbcopyResultFileNameExpect := fmt.Sprintf("resources/%s.cbcopy.check.output%s.expect", objectType, scenario)
		return checkCbcopyResultFileNameExpect
	}
	return ""
}

func executeScriptThenCheck(sourceHost string, sourcePort int, sourceUser, sourceDb, createScriptFileName, checkScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect string) {
	testutils.ExecuteSQLFileV2(sourceHost, sourcePort, sourceUser, sourceDb, createScriptFileName)

	checkResult(sourceHost, sourcePort, sourceUser, sourceDb, checkScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect)
}

func arrayContains(arrayToCheck []string, itemToCheck string) bool {
	for _, currentItem := range arrayToCheck {
		if currentItem == itemToCheck {
			return true
		}
	}
	return false
}

func caseInit(currentCaseId string) {
	fmt.Println("")
	fmt.Println("--> case START" + fmt.Sprintf(" - (%s) [%s]", currentCaseId, CurrentSpecReport().FullText()))

	context_currentCaseId = currentCaseId

	if arrayContains(runCaseList, currentCaseId) {
		runSkipCaseList = append(runSkipCaseList, currentCaseId)
		fmt.Println("    -- SKip This Case, invalid case id")
		Skip("")
	}
	runCaseList = append(runCaseList, currentCaseId)

	if arrayContains(userRequestSkipCaseList, currentCaseId) {
		runSkipCaseList = append(runSkipCaseList, currentCaseId)
		fmt.Println("    -- SKip This Case, because it is in user request skip case list")
		Skip("")
	}

	if (len(userRequestRunCaseList) > 0) && !arrayContains(userRequestRunCaseList, currentCaseId) {
		runSkipCaseList = append(runSkipCaseList, currentCaseId)
		fmt.Println("    -- SKip This Case, because it's not in user request run case list")
		Skip("")
	}
}

var _ = Describe("cbcopy end to end tests", func() {
	BeforeEach(func() {
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			runFailCaseList = append(runFailCaseList, context_currentCaseId)
			fmt.Println("<-- case END, FAIL")
		} else {
			if !arrayContains(runSkipCaseList, context_currentCaseId) {
				runPassCaseList = append(runPassCaseList, context_currentCaseId)
				fmt.Println("<-- case END, PASS")
			} else {
				fmt.Println("<-- case END, SKIP")
			}
		}
	})

	Describe(", scenario: ", func() {

		It("all types objects", func() {
			caseInit("1")

			fmt.Println("\n    -- create objects in source db --")

			objectType := "all_types_objects"

			createScriptFileName, checkScriptFileName, dropScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect := initFileNames(objectType, "")

			// this case (all types of objects) has many types of objects, some feature not in old version, expect file is hard to find a same one. some are same, could see if the expect file is symbol link.
			checkCreateResultFileNameExpect = checkCreateResultFileNameExpect + "." + versionSource
			checkDropResultFileNameExpectSource = checkDropResultFileNameExpectSource + "." + versionSource
			checkDropResultFileNameExpectTarget = checkDropResultFileNameExpectTarget + "." + versionTarget // indeed, target is just 3x and to CBDB that we only support to 3x and to cbdb.
			checkCbcopyResultFileNameExpect = checkCbcopyResultFileNameExpect + "." + versionSource + ".to" + "." + versionTarget

			defer cleanupObjects(dropScriptFileName, checkScriptFileName, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget)

			executeScriptThenCheck(sourceHost, sourcePort, sourceUser, sourceDb, createScriptFileName, checkScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect)

			fmt.Println("\n    -- cbcopy from source db to target db --")

			// if different host, let's copy global meta (e.g. role, resource group) too, so same cbcopy output expect could be used
			if sourceHost != targetHost {
				cbcopy(
					fmt.Sprintf("--source-host=%s", sourceHost), fmt.Sprintf("--source-port=%d", sourcePort), fmt.Sprintf("--source-user=%s", sourceUser),
					fmt.Sprintf("--dest-host=%s", targetHost), fmt.Sprintf("--dest-port=%d", targetPort), fmt.Sprintf("--dest-user=%s", targetUser),
					fmt.Sprintf("--dbname=%s", sourceDb),
					fmt.Sprintf("--dest-dbname=%s", targetDb),
					"--with-globalmeta",
					"--truncate")
			} else {
				cbcopy(
					fmt.Sprintf("--source-host=%s", sourceHost), fmt.Sprintf("--source-port=%d", sourcePort), fmt.Sprintf("--source-user=%s", sourceUser),
					fmt.Sprintf("--dest-host=%s", targetHost), fmt.Sprintf("--dest-port=%d", targetPort), fmt.Sprintf("--dest-user=%s", targetUser),
					fmt.Sprintf("--dbname=%s", sourceDb),
					fmt.Sprintf("--dest-dbname=%s", targetDb),
					"--truncate")
			}

			fmt.Println("\n    -- check objects in target db --")

			checkResult(targetHost, targetPort, targetUser, targetDb, checkScriptFileName, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect)
		})

		It("partition table", func() {
			caseInit("2")

			fmt.Println("\n    -- create objects in source db --")

			objectType := "partition_table_objects"

			createScriptFileName, checkScriptFileName, dropScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect := initFileNames(objectType, "")

			defer cleanupObjects(dropScriptFileName, checkScriptFileName, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget)

			executeScriptThenCheck(sourceHost, sourcePort, sourceUser, sourceDb, createScriptFileName, checkScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect)

			fmt.Println("\n    -- cbcopy from source db to target db --")

			cbcopy(
				fmt.Sprintf("--source-host=%s", sourceHost), fmt.Sprintf("--source-port=%d", sourcePort), fmt.Sprintf("--source-user=%s", sourceUser),
				fmt.Sprintf("--dest-host=%s", targetHost), fmt.Sprintf("--dest-port=%d", targetPort), fmt.Sprintf("--dest-user=%s", targetUser),
				fmt.Sprintf("--dbname=%s", sourceDb),
				fmt.Sprintf("--dest-dbname=%s", targetDb),
				"--truncate")

			fmt.Println("\n    -- check objects in target db --")

			checkResult(targetHost, targetPort, targetUser, targetDb, checkScriptFileName, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect)
		})

		It("filter - schema", func() {
			caseInit("3")

			fmt.Println("\n    -- create objects in source db --")

			objectType := "tables_objects"

			createScriptFileName, checkScriptFileName, dropScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect := initFileNames(objectType, ".filter.schema")

			defer cleanupObjects(dropScriptFileName, checkScriptFileName, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget)

			executeScriptThenCheck(sourceHost, sourcePort, sourceUser, sourceDb, createScriptFileName, checkScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect)

			fmt.Println("\n    -- cbcopy from source db to target db --")

			//	cbcopy --verbose --source-host=localhost --source-port=5432 --source-user=gpadmin --dbname=db1 --dest-host=localhost --dest-port=5432 --dest-user=gpadmin --dest-dbname=db1_copy --schema=db1.schema2 --truncate]
			//	[CRITICAL]:-The following flags may not be specified together: full, dbname, schema, include-table, include-table-file, global-metadata-only, schema-mapping-file

			//	cbcopy --verbose --source-host=localhost --source-port=5432 --source-user=gpadmin -dest-host=localhost --dest-port=5432 --dest-user=gpadmin --dest-dbname=db1_copy --schema=db1.schema2 --truncate
			//	[CRITICAL]:-Option[s] "--dest-dbname" only supports with option "--dbname or --include-table-file"

			cbcopy(
				fmt.Sprintf("--source-host=%s", sourceHost), fmt.Sprintf("--source-port=%d", sourcePort), fmt.Sprintf("--source-user=%s", sourceUser),
				fmt.Sprintf("--dest-host=%s", targetHost), fmt.Sprintf("--dest-port=%d", targetPort), fmt.Sprintf("--dest-user=%s", targetUser),
				fmt.Sprintf("--schema=%s.%s", sourceDb, "schema2"),
				fmt.Sprintf("--dest-schema=%s.%s", targetDb, "schema2"),
				"--truncate")

			fmt.Println("\n    -- check objects in target db --")

			checkResult(targetHost, targetPort, targetUser, targetDb, checkScriptFileName, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect)

		})

		It("filter - include table", func() {
			caseInit("4")

			fmt.Println("\n    -- create objects in source db --")

			objectType := "tables_objects"

			createScriptFileName, checkScriptFileName, dropScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect := initFileNames(objectType, ".filter.table.include")

			// at table level, cbcopy does not create db/table if not exist, so we need to create it in advance
			fmt.Println("\n    -- create objects in target db --")

			runCommand("dropdb", targetDb)
			runCommand("createdb", targetDb)

			defer cleanupObjects(dropScriptFileName, checkScriptFileName, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget)

			executeScriptThenCheck(sourceHost, sourcePort, sourceUser, sourceDb, createScriptFileName, checkScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect)

			executeScriptThenCheck(targetHost, targetPort, targetUser, targetDb, createScriptFileName, checkScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect)

			fmt.Println("\n    -- truncate table in target db --")

			truncateScriptFileName := fmt.Sprintf("resources/%s.truncate.sql", objectType)
			checkTruncateResultFileName := fmt.Sprintf("%s.truncate.check.output", objectType)
			checkTruncateResultFileNameExpect := fmt.Sprintf("resources/%s.truncate.check.output.expect", objectType)

			executeScriptThenCheck(targetHost, targetPort, targetUser, targetDb, truncateScriptFileName, checkScriptFileName, checkTruncateResultFileName, checkTruncateResultFileNameExpect)

			fmt.Println("\n    -- cbcopy from source db to target db --")

			cbcopy(
				fmt.Sprintf("--source-host=%s", sourceHost), fmt.Sprintf("--source-port=%d", sourcePort), fmt.Sprintf("--source-user=%s", sourceUser),
				fmt.Sprintf("--dest-host=%s", targetHost), fmt.Sprintf("--dest-port=%d", targetPort), fmt.Sprintf("--dest-user=%s", targetUser),
				fmt.Sprintf("--include-table=%s.%s.%s", sourceDb, "schema2", "t22"),
				fmt.Sprintf("--dest-table=%s.%s.%s", targetDb, "schema2", "t22"),
				"--truncate")

			fmt.Println("\n    -- check objects in target db --")

			checkResult(targetHost, targetPort, targetUser, targetDb, checkScriptFileName, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect)

		})

		It("filter - exclude table, table name directly", func() {
			caseInit("5")

			fmt.Println("\n    -- create objects in source db --")

			objectType := "tables_objects"

			createScriptFileName, checkScriptFileName, dropScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect := initFileNames(objectType, ".filter.table.exclude.direct")

			defer cleanupObjects(dropScriptFileName, checkScriptFileName, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget)

			executeScriptThenCheck(sourceHost, sourcePort, sourceUser, sourceDb, createScriptFileName, checkScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect)

			fmt.Println("\n    -- cbcopy from source db to target db --")

			cbcopy(
				fmt.Sprintf("--source-host=%s", sourceHost), fmt.Sprintf("--source-port=%d", sourcePort), fmt.Sprintf("--source-user=%s", sourceUser),
				fmt.Sprintf("--dest-host=%s", targetHost), fmt.Sprintf("--dest-port=%d", targetPort), fmt.Sprintf("--dest-user=%s", targetUser),
				fmt.Sprintf("--dbname=%s", sourceDb),
				fmt.Sprintf("--dest-dbname=%s", targetDb),
				fmt.Sprintf("--exclude-table=%s.%s.%s", sourceDb, "schema2", "t22"),
				"--truncate")

			fmt.Println("\n    -- check objects in target db --")

			checkResult(targetHost, targetPort, targetUser, targetDb, checkScriptFileName, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect)

		})

		It("filter - exclude table, table name filelist", func() {
			caseInit("6")

			fmt.Println("\n    -- create objects in source db --")

			objectType := "tables_objects"

			createScriptFileName, checkScriptFileName, dropScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect := initFileNames(objectType, ".filter.table.exclude.file")

			defer cleanupObjects(dropScriptFileName, checkScriptFileName, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget)

			executeScriptThenCheck(sourceHost, sourcePort, sourceUser, sourceDb, createScriptFileName, checkScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect)

			fmt.Println("\n    -- cbcopy from source db to target db --")

			cbcopy(
				fmt.Sprintf("--source-host=%s", sourceHost), fmt.Sprintf("--source-port=%d", sourcePort), fmt.Sprintf("--source-user=%s", sourceUser),
				fmt.Sprintf("--dest-host=%s", targetHost), fmt.Sprintf("--dest-port=%d", targetPort), fmt.Sprintf("--dest-user=%s", targetUser),
				fmt.Sprintf("--dbname=%s", sourceDb),
				fmt.Sprintf("--dest-dbname=%s", targetDb),
				fmt.Sprintf("--exclude-table-file=%s", "resources/table_objects.exclude_table.list"),
				"--truncate")

			fmt.Println("\n    -- check objects in target db --")

			checkResult(targetHost, targetPort, targetUser, targetDb, checkScriptFileName, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect)

		})

		It("option --append then --truncate", func() {
			caseInit("7")

			fmt.Println("\n    -- create objects in source db --")

			objectType := "tables_objects"

			createScriptFileName, checkScriptFileName, dropScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect := initFileNames(objectType, ".filter.schema")

			defer cleanupObjects(dropScriptFileName, checkScriptFileName, checkDropResultFileName, checkDropResultFileNameExpectSource, checkDropResultFileNameExpectTarget)

			executeScriptThenCheck(sourceHost, sourcePort, sourceUser, sourceDb, createScriptFileName, checkScriptFileName, checkCreateResultFileName, checkCreateResultFileNameExpect)

			fmt.Println("\n    -- cbcopy from source db to target db --")

			cbcopy(
				fmt.Sprintf("--source-host=%s", sourceHost), fmt.Sprintf("--source-port=%d", sourcePort), fmt.Sprintf("--source-user=%s", sourceUser),
				fmt.Sprintf("--dest-host=%s", targetHost), fmt.Sprintf("--dest-port=%d", targetPort), fmt.Sprintf("--dest-user=%s", targetUser),
				fmt.Sprintf("--schema=%s.%s", sourceDb, "schema2"),
				fmt.Sprintf("--dest-schema=%s.%s", targetDb, "schema2"),
				"--append")

			fmt.Println("\n    -- check objects in target db --")

			checkResult(targetHost, targetPort, targetUser, targetDb, checkScriptFileName, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect)

			// 2nd, cbcopy

			fmt.Println("\n    -- cbcopy from source db to target db, again, with --append --")

			cbcopy(
				fmt.Sprintf("--source-host=%s", sourceHost), fmt.Sprintf("--source-port=%d", sourcePort), fmt.Sprintf("--source-user=%s", sourceUser),
				fmt.Sprintf("--dest-host=%s", targetHost), fmt.Sprintf("--dest-port=%d", targetPort), fmt.Sprintf("--dest-user=%s", targetUser),
				fmt.Sprintf("--schema=%s.%s", sourceDb, "schema2"),
				fmt.Sprintf("--dest-schema=%s.%s", targetDb, "schema2"),
				"--append")

			fmt.Println("\n    -- check objects in target db --")
			checkCbcopyResultFileNameExpect = getFileName(objectType, "checkCbcopyResultFileNameExpect", ".filter.schema.append")
			checkResult(targetHost, targetPort, targetUser, targetDb, checkScriptFileName, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect)

			// 3rd, cbcopy

			fmt.Println("\n    -- cbcopy from source db to target db, again, with --truncate --")

			cbcopy(
				fmt.Sprintf("--source-host=%s", sourceHost), fmt.Sprintf("--source-port=%d", sourcePort), fmt.Sprintf("--source-user=%s", sourceUser),
				fmt.Sprintf("--dest-host=%s", targetHost), fmt.Sprintf("--dest-port=%d", targetPort), fmt.Sprintf("--dest-user=%s", targetUser),
				fmt.Sprintf("--schema=%s.%s", sourceDb, "schema2"),
				fmt.Sprintf("--dest-schema=%s.%s", targetDb, "schema2"),
				"--truncate")

			fmt.Println("\n    -- check objects in target db --")
			checkCbcopyResultFileNameExpect = getFileName(objectType, "checkCbcopyResultFileNameExpect", ".filter.schema")
			checkResult(targetHost, targetPort, targetUser, targetDb, checkScriptFileName, checkCbcopyResultFileName, checkCbcopyResultFileNameExpect)

		})

	})

})
