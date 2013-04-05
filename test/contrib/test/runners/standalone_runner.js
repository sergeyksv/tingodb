var Runner = require('integra').Runner;

module.exports = function(configurations) {
  //
  //  Single server runner
  //
  //

  // Configure a Run of tests
  var functional_tests_runner = Runner
    // Add configurations to the test runner
    .configurations(configurations)
    .exeuteSerially(true)
    // First parameter is test suite name
    // Second parameter is the configuration used
    // Third parameter is the list of files to execute
    .add("functional_tests",
      [
		'/test/tests/functional/find_tests.js'
        , '/test/tests/functional/insert_tests.js'
        , '/test/tests/functional/cursor_tests.js'         
      ]
    );

  //
  //  Single server auth
  //
  //

  // Configure a Run of tests
  var auth_single_server_runner = Runner
    // Add configurations to the test runner
    .configurations(configurations)
    .exeuteSerially(true)
    // First parameter is test suite name
    // Second parameter is the configuration used
    // Third parameter is the list of files to execute
    .add("single_server_auth",
      [
          '/test/tests/authentication/authentication_tests.js'
      ]
    );

  // Export runners
  return {
      runner: functional_tests_runner
    , runner_auth: auth_single_server_runner
  }    
}
