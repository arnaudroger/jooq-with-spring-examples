package net.petrikainulainen.jooqtips.student;

import org.jooq.DSLContext;
import org.simpleflatmapper.jdbc.JdbcMapper;
import org.simpleflatmapper.jdbc.JdbcMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static net.petrikainulainen.jooqtips.db.Tables.BOOKS;
import static net.petrikainulainen.jooqtips.db.Tables.STUDENTS;

/**
 * Provides finder methods used to query the student information.
 */
@Repository
class StudentRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(StudentRepository.class);

    private final DSLContext jooq;
    
    private final JdbcMapper<StudentDTO> studentMapper =
            JdbcMapperFactory
                    .newInstance()
                    .addKeys("id", "books_id")
                    .newMapper(StudentDTO.class);

    @Autowired
    StudentRepository(DSLContext jooq) {
        this.jooq = jooq;
    }

    /**
     * Finds all students.
     * @return  A list that contains all students found from the database.
     *          If no students is found, this method returns an empty list.
     * @throws DataQueryException   when the {@code ResultSet} object cannot be transformed into a list
     *                              because an exception was thrown.
     */
    @Transactional(readOnly = true)
    public List<StudentDTO> findAll() {
        LOGGER.info("Finding all students");

        try (ResultSet rs = jooq.select(STUDENTS.ID,
                STUDENTS.NAME,
                BOOKS.ID.as("books_id"),
                BOOKS.NAME.as("books_name")
                )
                .from(STUDENTS)
                .leftJoin(BOOKS).on(BOOKS.STUDENT_ID.eq(STUDENTS.ID))
                .orderBy(STUDENTS.ID.asc())
                .fetchResultSet()) {

            List<StudentDTO> students = new ArrayList<>();
            studentMapper.forEach(rs, students::add);
            LOGGER.info("Found {} students", students.size());
            
            return students;
        } catch (SQLException ex) {
            LOGGER.error("Cannot transform query result into a list because an error occurred", ex);
            throw new DataQueryException("Cannot transform query result into a list because an error occurred", ex);
        }
    }

    /**
     * Finds the information of the requested student.
     * @param id    The id of the requested student.
     * @return      An {@code Optional} that contains the found student.
     *              If no student is found, this method returns an empty
     *              {@code Optional} object.
     */
    @Transactional(readOnly = true)
    public Optional<StudentDTO> findById(Long id) {
        LOGGER.info("Finding student by id: {}", id);

        try (ResultSet rs = jooq.select(STUDENTS.ID,
                STUDENTS.NAME,
                BOOKS.ID.as("books_id"),
                BOOKS.NAME.as("books_name")
                )
                .from(STUDENTS)
                .leftJoin(BOOKS).on(BOOKS.STUDENT_ID.eq(STUDENTS.ID))
                .where(STUDENTS.ID.eq(id))
                .fetchResultSet()) {

            Iterator<StudentDTO> iterator = studentMapper.iterator(rs);

            if (!iterator.hasNext()) {
                return Optional.empty();
            }

            Optional<StudentDTO> student = Optional.of(iterator.next());

            if (iterator.hasNext()) {
                throw new DataQueryException(
                        "Cannot transform query result into an object because more than one student was found"
                );
            }

            LOGGER.info("Found student: {}", student);
            return student;

        } catch (SQLException ex) {
            LOGGER.error("Cannot transform result into an object because an error occurred", ex);
            throw new DataQueryException("Cannot transform result into an object because an error occurred", ex);
        }
    }




}
