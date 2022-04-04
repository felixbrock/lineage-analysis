# lineage-analysis

@Nasir: npm i should do the trick. No need to got through setup below.

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



