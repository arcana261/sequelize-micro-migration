"use strict";

const MicroMigration = require('../index');
const expect = require('chai').expect;
const Sequelize = require('sequelize');
const task = require('xcane').task;
const iterable = require('xcane').iterable;
const path = require('path');
const clone = require('clone');

let overrideFs = {
  _files: [],
  _lastDir: null,
  _storage: {},
  _upped: {},
  readdir: dir => {
    overrideFs._lastDir = dir;
    return Promise.resolve(overrideFs._files);
  },
  _require: x => overrideFs._storage[x]
};

const filesSorted = [
  '201601011200-AddPerson.js',
  '201601101200-AddName.js',
  '2016011012001-AddAge.js',
  '201602011403-RemoveAge.js'
];

overrideFs._storage[path.join(__dirname, filesSorted[0].replace(/\.js$/g, ''))] = {
  up: (queryInterface, sequelize) => task.spawn(function* () {
    expect(overrideFs._upped[filesSorted[0]]).to.not.be.true;
    expect(overrideFs._upped[filesSorted[1]]).to.not.be.true;
    expect(overrideFs._upped[filesSorted[2]]).to.not.be.true;
    expect(overrideFs._upped[filesSorted[3]]).to.not.be.false;

    yield queryInterface.createTable('people', {
      id: {
        type: sequelize.Sequelize.INTEGER,
        primaryKey: true,
        autoIncrement: true
      }
    });

    overrideFs._upped[filesSorted[0]] = true;
  }),

  down: (queryInterface, sequelize) => task.spawn(function* () {
    expect(overrideFs._upped[filesSorted[0]]).to.be.true;
    expect(overrideFs._upped[filesSorted[1]]).to.not.be.true;
    expect(overrideFs._upped[filesSorted[2]]).to.not.be.true;
    expect(overrideFs._upped[filesSorted[3]]).to.not.be.true;

    yield queryInterface.dropTable('people');
    overrideFs._upped[filesSorted[0]] = false;
  })
};

overrideFs._storage[path.join(__dirname, filesSorted[1].replace(/\.js$/g, ''))] = {
  up: (queryInterface, sequelize) => task.spawn(function* () {
    expect(overrideFs._upped[filesSorted[0]]).to.be.true;
    expect(overrideFs._upped[filesSorted[1]]).to.not.be.true;
    expect(overrideFs._upped[filesSorted[2]]).to.not.be.true;
    expect(overrideFs._upped[filesSorted[3]]).to.not.be.true;

    yield queryInterface.addColumn('people', 'name', {
      type: Sequelize.TEXT
    });
    overrideFs._upped[filesSorted[1]] = true;
  }),

  down: (queryInterface, sequelize) => task.spawn(function* () {
    expect(overrideFs._upped[filesSorted[0]]).to.be.true;
    expect(overrideFs._upped[filesSorted[1]]).to.be.true;
    expect(overrideFs._upped[filesSorted[2]]).to.not.be.true;
    expect(overrideFs._upped[filesSorted[3]]).to.not.be.true;

    yield queryInterface.removeColumn('people', 'name');
    overrideFs._upped[filesSorted[1]] = false;
  })
};

overrideFs._storage[path.join(__dirname, filesSorted[2].replace(/\.js$/g, ''))] = {
  up: (queryInterface, sequelize) => task.spawn(function* () {
    expect(overrideFs._upped[filesSorted[0]]).to.be.true;
    expect(overrideFs._upped[filesSorted[1]]).to.be.true;
    expect(overrideFs._upped[filesSorted[2]]).to.not.be.true;
    expect(overrideFs._upped[filesSorted[3]]).to.not.be.true;
    yield queryInterface.addColumn('people', 'age', {
      type: Sequelize.INTEGER
    });
    overrideFs._upped[filesSorted[2]] = true;
  }),

  down: (queryInterface, sequelize) => task.spawn(function* () {
    expect(overrideFs._upped[filesSorted[0]]).to.be.true;
    expect(overrideFs._upped[filesSorted[1]]).to.be.true;
    expect(overrideFs._upped[filesSorted[2]]).to.be.true;
    expect(overrideFs._upped[filesSorted[3]]).to.not.be.true;

    yield queryInterface.removeColumn('people', 'age');
    overrideFs._upped[filesSorted[2]] = false;
  })
};

overrideFs._storage[path.join(__dirname, filesSorted[3].replace(/\.js$/g, ''))] = {
  up: (queryInterface, sequelize) => task.spawn(function* () {
    expect(overrideFs._upped[filesSorted[0]]).to.be.true;
    expect(overrideFs._upped[filesSorted[1]]).to.be.true;
    expect(overrideFs._upped[filesSorted[2]]).to.be.true;
    expect(overrideFs._upped[filesSorted[3]]).to.not.be.true;
    yield queryInterface.removeColumn('people', 'age');
    overrideFs._upped[filesSorted[3]] = true;
  }),

  down: (queryInterface, sequelize) => task.spawn(function* () {
    expect(overrideFs._upped[filesSorted[0]]).to.be.true;
    expect(overrideFs._upped[filesSorted[1]]).to.be.true;
    expect(overrideFs._upped[filesSorted[2]]).to.be.true;
    expect(overrideFs._upped[filesSorted[3]]).to.be.true;

    yield queryInterface.addColumn('people', 'age', {
      type: Sequelize.INTEGER
    });
    overrideFs._upped[filesSorted[3]] = false;
  })
};

let filesPermuted = filesSorted;
while (!(filesPermuted < filesSorted) && !(filesPermuted > filesSorted)) {
  filesPermuted = iterable.from(filesSorted).permute().toArray();
}

describe('SequelizeMicroMigration', () => {
  let sequelize = null;
  let migration = null;

  beforeEach(done => {
    sequelize = null;
    migration = null;

    sequelize = new Sequelize({
      dialect: 'sqlite',
      storage: ':memory:',
      benchmark: true
    });

    MicroMigration._overrideFs(overrideFs);
    MicroMigration._overrideRequire(overrideFs._require);
    migration = new MicroMigration(sequelize, 'myApplication', __dirname);
    overrideFs._lastDir = null;
    overrideFs._upped = {};

    sequelize.sync().then(() => done()).catch(done);
  });

  describe('#current()', () => {
    it('should correctly show version of empty database', () =>
      task.spawn(function* () {
        expect(yield migration.current()).to.be.equal('0');
      }));
  });

  describe('#listUp()', () => {
    it('should return correct list on empty database, sorted', () =>
      task.spawn(function* () {
        overrideFs._files = filesSorted;
        expect(yield migration.listUp()).to.be.deep.equal(
          filesSorted.map(x => [x.replace(/\.js$/g, ''), 'up']));
        expect(yield migration.listUp()).to.be.deep.equal(
          filesSorted.map(x => [x.replace(/\.js$/g, ''), 'up']));
        expect(overrideFs._lastDir).to.be.equal(__dirname);
      }));

    it('should return correct list on empty database, permuted', () =>
      task.spawn(function* () {
        overrideFs._files = filesPermuted;
        expect(filesPermuted).to.not.be.deep.equal(filesSorted);
        expect(yield migration.listUp()).to.be.deep.equal(
          filesSorted.map(x => [x.replace(/\.js$/g, ''), 'up']));
        expect(yield migration.listUp()).to.be.deep.equal(
          filesSorted.map(x => [x.replace(/\.js$/g, ''), 'up']));
        expect(overrideFs._lastDir).to.be.equal(__dirname);
      }));

    it('should return correct list step-wise on empty database, sorted', () =>
      task.spawn(function* () {
        overrideFs._files = filesSorted;

        for (let i = 0; i < filesSorted.length; i++) {
          expect(yield migration.listUp(filesSorted[i].replace(/\.js$/g, '')))
            .to.be.deep.equal(filesSorted.slice(0, i + 1).map(
              x => [x.replace(/\.js$/g, ''), 'up']));
        }
      }));

    it('should return correct list step-wise on empty database, permuted', () =>
      task.spawn(function* () {
        overrideFs._files = filesPermuted;

        for (let i = 0; i < filesSorted.length; i++) {
          expect(yield migration.listUp(filesSorted[i].replace(/\.js$/g, '')))
            .to.be.deep.equal(filesSorted.slice(0, i + 1).map(
              x => [x.replace(/\.js$/g, ''), 'up']));
        }
      }));
  });

  describe('#listDown()', () => {
    it('should return correct list on empty database, sorted', () =>
      task.spawn(function* () {
        overrideFs._files = filesSorted;
        expect(yield migration.listDown()).to.be.deep.equal([]);
        expect(yield migration.listDown()).to.be.deep.equal([]);
        expect(overrideFs._lastDir).to.be.equal(__dirname);
      }));

    it('should return correct list on empty database, permuted', () =>
      task.spawn(function* () {
        overrideFs._files = filesPermuted;
        expect(yield migration.listDown()).to.be.deep.equal([]);
        expect(yield migration.listDown()).to.be.deep.equal([]);
        expect(overrideFs._lastDir).to.be.equal(__dirname);
      }));

    it('should return correct list step-wise on empty database, sorted', () =>
      task.spawn(function* () {
        overrideFs._files = filesSorted;

        for (let i = 0; i < filesSorted.length; i++) {
          expect(yield migration.listDown(filesSorted[i].replace(/\.js$/g, '')))
            .to.be.deep.equal(filesSorted.slice(0, i + 1).map(
              x => [x.replace(/\.js$/g, ''), 'up']));
        }
      }));

    it('should return correct list step-wise on empty database, permuted', () =>
      task.spawn(function* () {
        overrideFs._files = filesPermuted;

        for (let i = 0; i < filesSorted.length; i++) {
          expect(yield migration.listDown(filesSorted[i].replace(/\.js$/g, '')))
            .to.be.deep.equal(filesSorted.slice(0, i + 1).map(
              x => [x.replace(/\.js$/g, ''), 'up']));
        }
      }));
  });

  describe('#up()', () => {
    it('should up one version from empty database', () =>
      task.spawn(function* () {
        overrideFs._files = filesSorted;

        expect(yield migration.current()).to.be.equal('0');
        expect(yield migration.listUp()).to.be.deep.equal(
          filesSorted.map(x => [x.replace(/\.js$/g, ''), 'up']));
        expect(overrideFs._upped[filesSorted[0]]).to.be.undefined;
        yield migration.up(filesSorted[0].replace(/\.js$/g, ''));
        expect(overrideFs._upped[filesSorted[0]]).to.be.true;
        expect(yield migration.current()).to.be.equal(filesSorted[0].replace(/\.js$/g, ''));
        expect(yield migration.listUp()).to.be.deep.equal(
          filesSorted.slice(1).map(x => [x.replace(/\.js$/g, ''), 'up']));

        const table = sequelize.define('person', {
          id: {
            type: Sequelize.INTEGER,
            primaryKey: true,
            autoIncrement: true
          }
        }, {
          timestamps: false
        });

        yield table.create({});
      }));

    it('should up one version with number', () =>
      task.spawn(function* () {
        overrideFs._files = filesSorted;
        expect(yield migration.current()).to.be.equal('0');
        yield migration.up(1);
        expect(overrideFs._upped[filesSorted[0]]).to.be.true;
        expect(yield migration.current()).to.be.equal(filesSorted[0].replace(/\.js$/g, ''));
      }));

    it('should be transactional', () =>
      task.spawn(function* () {
        let newStorage = clone(overrideFs._storage);
        newStorage[path.join(__dirname, filesSorted[0].replace(/\.js$/g, ''))] = {
          up: (queryInterface, sequelize) => task.spawn(function* () {
            expect(yield migration.current()).to.be.equal('0');
            expect(yield migration.listUp()).to.be.deep.equal(
              filesSorted.map(x => [x.replace(/\.js$/g, ''), 'up']));
            expect(overrideFs._upped[filesSorted[0]]).to.be.undefined;
            yield overrideFs._storage[path.join(__dirname, filesSorted[0].replace(/\.js$/g, ''))].up(queryInterface, sequelize);
            expect(overrideFs._upped[filesSorted[0]]).to.be.true;
            expect(yield queryInterface.showAllTables()).to.include('people');
            throw new Error('hey! abort!');
          })
        };
        MicroMigration._overrideRequire(x => newStorage[x]);

        expect(yield migration.current()).to.be.equal('0');
        expect(yield migration.listUp()).to.be.deep.equal(
          filesSorted.map(x => [x.replace(/\.js$/g, ''), 'up']));
        expect(overrideFs._upped[filesSorted[0]]).to.be.undefined;

        try {
          yield migration.up(filesSorted[0].replace(/\.js$/g, ''));
          throw new Error('not thrown');
        } catch(err) {
          expect(err).to.be.an.instanceof(Error);
          expect(err.message).to.be.equal('hey! abort!');
          expect(yield sequelize.getQueryInterface().showAllTables()).to.not.include('people');
          expect(yield migration.current()).to.be.equal('0');
          expect(yield migration.listUp()).to.be.deep.equal(
            filesSorted.map(x => [x.replace(/\.js$/g, ''), 'up']));
        }
      }));

      it('should apply all migrations', () =>
        task.spawn(function* () {
          overrideFs._files = filesSorted;
          yield migration.up();
          expect(overrideFs._upped[filesSorted[0]]).to.be.true;
          expect(overrideFs._upped[filesSorted[1]]).to.be.true;
          expect(overrideFs._upped[filesSorted[2]]).to.be.true;
          expect(overrideFs._upped[filesSorted[3]]).to.be.true;
        }));

      it('should work with multiple numebers', () =>
        task.spawn(function* () {
          overrideFs._files = filesSorted;
          yield migration.up(1);
          expect(overrideFs._upped[filesSorted[0]]).to.be.true;
          expect(overrideFs._upped[filesSorted[1]]).to.not.be.true;
          expect(overrideFs._upped[filesSorted[2]]).to.not.be.true;
          expect(overrideFs._upped[filesSorted[3]]).to.not.be.true;
          yield migration.up(2);
          expect(overrideFs._upped[filesSorted[0]]).to.be.true;
          expect(overrideFs._upped[filesSorted[1]]).to.be.true;
          expect(overrideFs._upped[filesSorted[2]]).to.be.true;
          expect(overrideFs._upped[filesSorted[3]]).to.not.be.true;
          yield migration.up(2);
          expect(overrideFs._upped[filesSorted[0]]).to.be.true;
          expect(overrideFs._upped[filesSorted[1]]).to.be.true;
          expect(overrideFs._upped[filesSorted[2]]).to.be.true;
          expect(overrideFs._upped[filesSorted[3]]).to.be.true;
        }));

      it('should be able to install missing migration', () =>
        task.spawn(function* () {
          overrideFs._files = filesSorted.filter((x, i) => i !== 1);
          expect(overrideFs._upped[filesSorted[0]]).to.not.be.true;
          expect(overrideFs._upped[filesSorted[1]]).to.not.be.true;
          expect(overrideFs._upped[filesSorted[2]]).to.not.be.true;
          expect(overrideFs._upped[filesSorted[3]]).to.not.be.true;
          yield migration.up(1);
          overrideFs._upped[filesSorted[1]] = true;
          yield migration.up();
          expect(overrideFs._upped[filesSorted[0]]).to.be.true;
          expect(overrideFs._upped[filesSorted[1]]).to.be.true;
          expect(overrideFs._upped[filesSorted[2]]).to.be.true;
          expect(overrideFs._upped[filesSorted[3]]).to.be.true;
          migration._clearCache();
          overrideFs._files = filesSorted;
          expect(yield migration.listUp()).to.be.deep.equal([
            [filesSorted[3].replace(/\.js$/g, ''), 'down'],
            [filesSorted[2].replace(/\.js$/g, ''), 'down'],
            [filesSorted[1].replace(/\.js$/g, ''), 'up'],
            [filesSorted[2].replace(/\.js$/g, ''), 'up'],
            [filesSorted[3].replace(/\.js$/g, ''), 'up']
          ]);
          expect(yield migration.listUp()).to.be.deep.equal([
            [filesSorted[3].replace(/\.js$/g, ''), 'down'],
            [filesSorted[2].replace(/\.js$/g, ''), 'down'],
            [filesSorted[1].replace(/\.js$/g, ''), 'up'],
            [filesSorted[2].replace(/\.js$/g, ''), 'up'],
            [filesSorted[3].replace(/\.js$/g, ''), 'up']
          ]);
        }));
  });

  describe('#down()', () => {
    it('should down one version to empty repository', () =>
      task.spawn(function* () {
        overrideFs._files = filesSorted;
        expect(yield sequelize.getQueryInterface().showAllTables()).to.not.include('people');
        expect(yield migration.current()).to.be.equal('0');
        yield migration.up(filesSorted[0].replace(/\.js$/g, ''));
        expect(yield sequelize.getQueryInterface().showAllTables()).to.include('people');
        expect(yield migration.current()).to.be.equal(filesSorted[0].replace(/\.js$/g, ''));
        expect(yield migration.listUp()).to.be.deep.equal(
          filesSorted.slice(1).map(x => [x.replace(/\.js$/g, ''), 'up']));
        expect(overrideFs._upped[filesSorted[0]]).to.be.true;
        yield migration.down(1);
        expect(yield sequelize.getQueryInterface().showAllTables()).to.not.include('people');
        expect(yield migration.current()).to.be.equal('0');
        expect(overrideFs._upped[filesSorted[0]]).to.be.false;
        expect(yield migration.listUp()).to.be.deep.equal(
          filesSorted.map(x => [x.replace(/\.js$/g, ''), 'up']));
      }));

    it('should be able to downgrade all migrations', () =>
      task.spawn(function* () {
        overrideFs._files = filesSorted;
        yield migration.up();
        expect(overrideFs._upped[filesSorted[0]]).to.be.true;
        expect(overrideFs._upped[filesSorted[1]]).to.be.true;
        expect(overrideFs._upped[filesSorted[2]]).to.be.true;
        expect(overrideFs._upped[filesSorted[3]]).to.be.true;
        yield migration.down();
        expect(yield sequelize.getQueryInterface().showAllTables()).to.not.include('people');
        expect(overrideFs._upped[filesSorted[0]]).to.be.false;
        expect(overrideFs._upped[filesSorted[1]]).to.be.false;
        expect(overrideFs._upped[filesSorted[2]]).to.be.false;
        expect(overrideFs._upped[filesSorted[3]]).to.be.false;
      }));

    it('should be able to work with numbers', () =>
      task.spawn(function* () {
        overrideFs._files = filesSorted;
        expect(yield migration.up());
        expect(overrideFs._upped[filesSorted[0]]).to.be.true;
        expect(overrideFs._upped[filesSorted[1]]).to.be.true;
        expect(overrideFs._upped[filesSorted[2]]).to.be.true;
        expect(overrideFs._upped[filesSorted[3]]).to.be.true;
        yield migration.down(2);
        expect(overrideFs._upped[filesSorted[0]]).to.be.true;
        expect(overrideFs._upped[filesSorted[1]]).to.be.true;
        expect(overrideFs._upped[filesSorted[2]]).to.be.false;
        expect(overrideFs._upped[filesSorted[3]]).to.be.false;
        yield migration.down(1);
        expect(overrideFs._upped[filesSorted[0]]).to.be.true;
        expect(overrideFs._upped[filesSorted[1]]).to.be.false;
        expect(overrideFs._upped[filesSorted[2]]).to.be.false;
        expect(overrideFs._upped[filesSorted[3]]).to.be.false;
        yield migration.down(2);
        expect(overrideFs._upped[filesSorted[0]]).to.be.false;
        expect(overrideFs._upped[filesSorted[1]]).to.be.false;
        expect(overrideFs._upped[filesSorted[2]]).to.be.false;
        expect(overrideFs._upped[filesSorted[3]]).to.be.false;
      }));

      it('should be able to install missing migration', () =>
        task.spawn(function* () {
          overrideFs._files = filesSorted.filter((x, i) => i !== 1);
          expect(overrideFs._upped[filesSorted[0]]).to.not.be.true;
          expect(overrideFs._upped[filesSorted[1]]).to.not.be.true;
          expect(overrideFs._upped[filesSorted[2]]).to.not.be.true;
          expect(overrideFs._upped[filesSorted[3]]).to.not.be.true;
          yield migration.up(1);
          overrideFs._upped[filesSorted[1]] = true;
          yield migration.up();
          expect(overrideFs._upped[filesSorted[0]]).to.be.true;
          expect(overrideFs._upped[filesSorted[1]]).to.be.true;
          expect(overrideFs._upped[filesSorted[2]]).to.be.true;
          expect(overrideFs._upped[filesSorted[3]]).to.be.true;
          migration._clearCache();
          overrideFs._files = filesSorted;
          expect(yield migration.listDown(1)).to.be.deep.equal([
            [filesSorted[3].replace(/\.js$/g, ''), 'down'],
            [filesSorted[2].replace(/\.js$/g, ''), 'down'],
            [filesSorted[1].replace(/\.js$/g, ''), 'up'],
            [filesSorted[2].replace(/\.js$/g, ''), 'up']
          ]);
          expect(yield migration.listDown(1)).to.be.deep.equal([
            [filesSorted[3].replace(/\.js$/g, ''), 'down'],
            [filesSorted[2].replace(/\.js$/g, ''), 'down'],
            [filesSorted[1].replace(/\.js$/g, ''), 'up'],
            [filesSorted[2].replace(/\.js$/g, ''), 'up']
          ]);
        }));
  });
});
