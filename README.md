# lineage-analysis

# selector

Setup
express: npm init (index.js, UNLICENSED)

typescript: npm i -D typescript

(template for tsconfig file)
typescript: npm install --save-dev @tsconfig/node14

npm i -D eslint-config-airbnb-base

INSTALL ALL: npm info "eslint-config-airbnb-base@latest" peerDependencies

CHECK FOR RIGHT VERSIONS AT: https://www.npmjs.com/package/eslint-config-airbnb-typescript
npm i -D eslint-config-airbnb-typescript
npm i -D @typescript-eslint/eslint-plugin@^???
npm i -D @typescript-eslint/parser@^???

(https://prettier.io/docs/en/install.html)
prettier: npm install --save-dev --save-exact prettier

(https://github.com/prettier/eslint-config-prettier#installation)
prettier: npm install --save-dev eslint-config-prettier

(https://thesoreon.com/blog/how-to-set-up-eslint-with-typescript-in-vs-code)
vs-code: "eslint.validate": ["typescript", "typescriptreact"]

Copy the following files of most recent microservice to new microservice. Some aspects might be outdated and need to be specifically defined to versions used in the new service:
.dockerignore
Dockerfile
.env
.eslintignore
.eslintrc.json
.gitignore
.prettierignore
.prettierrc.json
tsconfig.json

Check what information (e.g. scripts, dev-dependencies, dependencies, license...) are stored in package.json should be copied to new project

---------------------------------
Security

https://stackoverflow.com/questions/23693796/where-should-i-keep-the-credentials-of-my-database

security check on repository level

------------------------------

Guideline
(https://www.typescriptlang.org/docs/handbook/typescript-in-5-minutes.html)
You’ll see that there are two syntaxes for building types: Interfaces and Types. You should prefer interface. Use type when you need specific features.

(https://www.typescriptlang.org/play?ssl=42&ssc=1&pln=46&pc=55&e=83#code/PTAEBUAsFMCdtAQ3qALgdwPagLaIJYB2ammANgM4mgAm0AxmcgqjKBZIgA4KYBmSQgFgAUCFCYARgCsGqAFygiqOH0T1oVRIRpoAnjyRl8iCpoB0okFbBRoepCgBucBxXw58TWABpBuvkxYNDYcTApUUHpMHDDielNNGyR6SNYECkQcaEsRUVQDBAAhfFgacELQAF5QAG9RUFB0IgBzCkUAJgBuUQBfHryRZVV1YtKaAElCFVg1DTqGptb20G6+0VFowgjQSXGARkUSsorDGtqlwjbO0H7NzG3IvbKOo-GpmbmEc8vr1duBskigxEABXMwQQoAZXosHwXEi+C07FQsFBqVBsEQZH0hgoegi0BwPmS+FQAHIqFxwu5JGQWNhhrBPAAPELQUqgcEMzC5e6PXbjADMbzKHxG8xqzxo+0BYlsMAckkwrHYoK41NgkWgLJUOlaEnSwSZXy0OlxFmSpwQ2JMZioNGwrCRoCcJiU0zgZlS+AeFoofnQkC80FJntmoyonBcSFAAGt7FgynyRAVDAB5dA4n6ETAY2CEbGKVGghC9UAAMlAx3KhQGaYQACUpERqnVQLn84WyIo1JQy5Xq+9w185SbRqAAArQdR5uOgHV6mhUGvWhYiRrRMiYTF8UE9tBo6ADRp8YyaXvYswDXqicfzADCwfoCeIi+gOhXw8+E-qG6i5A7rM+6Xv2J6gGe+AXhBV7Husgz0pEmBZoombZu2zRXCsHR+J2qCYt2xZHgCoiIVEz6vooT74C+H5thcmF-DhAHbruIEwf2fiQdBfYQncgziFAiCRJk+A0IGCDwNEsQfroeg7lyEL3poEguMEDbJLaiQUOYoBQjw9D4HwNHYmQeh+JIILcqA8mgksZA4i00BPM5MwLrAsBBMk2QUJkTk6aAEwCLZoCQJgamqXA7IQeQ26Me5nmwAGNkKQkwjyuw0AIGF6CQjwMJwgiUTaKAXCeW6dBoF6UXaLoYQoIE9DcjQ3maH5KlBnRSZxgazSqspVDGAmoDUbRhApsh6H0BRH4DNNNGvm2k1yuI6aEAgeDSEEtBGXwcAfvMlkYFlxANkYdoqU4VADZpKCsMJHo-hoWgoJgPDELVFrndpjgIIw4TQDQuSCcGVDZNoVAhWlC66rJgiPRKCCSA4dCMMgBpkppmVbLoqAeDkGxDCOE4ANJknq66NFwmJJYoyrkDOhA3oTymgGTqAU3+m6AZiigRHCVzMwJYBTNFKowMECQQogX1pbmLnkdoTm6DuqDuJV-CkmrtAMN4wm+uNhNnZO6pcA45yLFuQR86irQ3nKxum+blMkASiiEKCOCWbA9uE+IAAi0DvTQBp+rZwQtJgV5+M6Dq7fthDzNE+66JZSDJNS7h4zG20y+tLT6y4ukABLIdAEWBME1N0jRZkw5ngPJA2yVkpSsZHW5CQOdQeDDekODw8pKaretEgCOkuyaJE8AUEBz0xcEZgclcRg4vw0WA05yRSypyA7uazeutdxPPTHoMouo87haocVN5A8CILoLoy5HmC6FwTDzKg2ARMgCh+2ASAHMuDtBAL-F819ZhxXMNJYAABHUsEQDYUGAEKAA7B0IU6ChQAFZgDN1hPCVAABaAaxCrrEObsAHBHQABsAAODoTCAAMABiGhDCmEdGYaIIAA)
// That said, we recommend you use interfaces over type
// aliases. Specifically, because you will get better error
// messages. If you hover over the following errors, you can
// see how TypeScript can provide terser and more focused
// messages when working with interfaces like Chicken.

(https://martinfowler.com/bliki/EvansClassification.html)
Entity Classification

(https://blog.cleancoder.com/uncle-bob/2016/01/04/ALittleArchitecture.html)
Clean Code Architecture

(https://github.com/Microsoft/TypeScript/wiki/Coding-guidelines#null-and-undefined)
ADD CODING GUIDELINES


Interface starting with 'I' if it is describing a class interface
Interface over type. type only for specific purposes like union type

if one line then do it (like 'if' things)

declare const and let type where complex types are used e.g. :Selector

use CRUD verbs when describing repo functions (rather than add or list)
  - if addressing a database use 'read'
  - if addressing an external API use 'get'

use normal function notation where possible and try to avoid storing functions in constants where possible


https://stackoverflow.com/questions/6845772/rest-uri-convention-singular-or-plural-name-of-resource-while-creating-it

https://docs.npmjs.com/specifying-dependencies-and-devdependencies-in-a-package-json-file
Bascially, all external libraries that are directly used by code (e.g. not eslint) need to be installed as dependencies (--save). Those need to be javascript libraries. The typescript equivalent need to be installed under devdependencies (--save-dev)

when naming classes of files use CRUD terminology rather than addSelectorRepository (instead createSelectorRepository)


Dependency Injection
https://blog.risingstack.com/dependency-injection-in-node-js/
https://medium.com/@Jeffijoe/dependency-injection-in-node-js-2016-edition-f2a88efdd427


Use-case specific Dtos should only be used in the particular use-case (CRUD) context. ReadAutomation shouldn't use CreateSubsriptionDto. However, CreateAutomation might use ReadAutomationDto if a automation is read from the db during the CreateAutomation use-case.


Be aware of the differences between entities and value-types
https://enterprisecraftsmanship.com/posts/entity-vs-value-object-the-ultimate-list-of-differences/

------Redefine-----------
Within microservice a layered architecture is followed since the limited scope allows this architecture style. Microservice ecosystem basically represents package-driven architecture

------Redefine-----------
TRY TO USE USE-CASES WHENEVER POSSIBLE. Try to understand how likely it is that code changes. the higher the likehood, the more use-cases should be used. If a repository functionality other than the entity's one it should happen over a corresponding use-case rather than the repository interface. However, if the corresponding action is too heavy to duplicate or is no use-case at all, than a corresponding repository interface might be called directly.

https://stackoverflow.com/questions/5068984/should-the-repository-layer-return-data-transfer-objects-dto
Input and output of repository functions have to be entities not dtos. Exception are external API calls that are not corresponding to local entity types?!

No repositories for value-objects. Only aggregate roots (which are entities)

Normally, value-object use-cases (like alert or warning use-cases) are stored in their own seperate folder, independet of its root-entity use-cases (like systems or selectors). However, when making API calls e.g. to get-system and post-warning only Dtos are reference and, hence, can stored in one common folder (like system-api).

https://stackoverflow.com/questions/25385559/rest-api-best-practices-args-in-query-string-vs-in-request-body#:~:text=Usually%20the%20content%20body%20is,specify%20the%20exact%20data%20requested.&text=in%20the%20body%20but%20when,some%20property%20of%20the%20files.
Usually the content body is used for the data that is to be uploaded/downloaded to/from the server and the query parameters are used to specify the exact data requested. If you are not uploading data in some form to persistence than query params should be used. E.g. create alert is providing the selectorId which will not be uploaded to the db rather than used to identify the corresponding selector to create an alert for.

Instead of returning rather throw errors in try block of try/catch

https://softwareengineering.stackexchange.com/questions/191596/is-it-ok-to-partially-change-a-collection-with-put-or-delete/266728
Say, you want to update a single product, you make a PUT to /products/{id}. If you want to partially update a single product (not updating every field), you could also use a PATCH to /products/{id}. The same goes for deletion of a single entity (DELETE to /products/{id}).

https://stackoverflow.com/questions/6845772/rest-uri-convention-singular-or-plural-name-of-resource-while-creating-it
Use singular when accessing a single resource e.g. automation/1213445 and plural when acessing multiple resouces e.g. automations

https://stackoverflow.com/questions/37980559/is-it-better-to-return-undefined-or-null-from-a-javascript-function
"there is no such object" is represented using null.
"there is no such property" is represented using undefined.
"there is no such element" is represented using null.

https://stackoverflow.com/questions/1969993/is-it-better-to-return-null-or-empty-collection
It is considered a best practice to NEVER return null when returning a collection or enumerable. ALWAYS return an empty enumerable/collection. It prevents the aforementioned nonsense, and prevents your car getting egged by co-workers and users of your classes.
https://stackoverflow.com/questions/1628434/null-objects-vs-empty-objects
I tend to be dubious of code with lots of NULLs, and try to refactor them away where possible with exceptions, empty collections, Java Optionals, and so on.

https://www.reddit.com/r/javascript/comments/7rv221/to_indexjs_or_not_to_indexjs/
Index.js is not used so far for overhead reasons due to the small scope of the microservices. However, if a benefit is expected out of the use they might be added at a later point

Terminology for providing and consuming api endpoints:
providing: resources (documents, classes, functions, declarations...) related to a specific endpoint should use the create, read, update, delete terms
consuming: resources (documents, classes, functions, declarations...) consuming specific endpoint should use the http method term used by the endpoint (get, post...)

Do not expose business logic to client. Be careful what errors to return when error handling
https://itnext.io/graceful-error-handling-in-rest-driven-web-applications-d4209b4937aa

http://callbackhell.com/

Use arrow function notation rather than function keyword

Environments
https://www.quora.com/What-is-the-difference-between-development-test-and-production-environment

Production environment - that one is pretty simple, it's an instance(or instances) on which your application for end users runs - it might be e.g. some AWS instance, Heroku etc.

Test environment - it tries to emulate production environment as much as possible, generally it's used for testing apllication before deploying it to production environment (e.g. making new version of you app available for users), checking if new features didn't introduce any regressions, etc.

Development environment - this one is also simple, it's environment on developer's machine, e.g. some VM.


Successful branching
https://nvie.com/posts/a-successful-git-branching-model/

https://www.acunetix.com/blog/web-security-zone/nosql-injections/
In the case of MongoDB, never use where, mapReduce, or group operators with user input because these operators allow the attacker to inject JavaScript and are therefore much more dangerous than others. For extra safety, set javascriptEnabled to false in mongod.conf, if possible.

Only use Result Wrapper for API calls (as a response of use cases)

Only use Dto's for outgoing data transfer, e.g. no need to return a modeldto object from create-model since this use case is not directly exposed to external services, but only internal ones




