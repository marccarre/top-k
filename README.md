# top-k
Implementation of "Efficient Computation of Frequent and Top-k Elements in Data Streams", by Metwally, Agrawal, and El Abbadi

## Build:

    gradle clean check

## Package:

    gradle clean check distZip installDist

## Test Coverage:

Test coverage report: `open build/reports/jacoco/index.html`
Measures which fraction of the production code is executed by the tests.

N.B.: the above does *NOT* assess anything about the quality of these tests, for this you would have to use, for example, [mutation testing](http://pitest.org/),
which would mutate production code and ensure that, for each mutant, at least one test fails, hence validating the fact tests assert state, post-conditions and outputs.
If you are interested in trying mutation testing on this project, please run: `gradle pitest`
Mutation coverage report: `open build/reports/pitest/yyyyMMddHHmm/index.html `
