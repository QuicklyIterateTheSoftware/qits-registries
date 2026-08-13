package eu.wohlben.qits.artifacts.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import java.time.Instant;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * The maven proxy's cached {@code maven-metadata.xml} for one upstream path.
 *
 * <p>The npm packument pattern verbatim, because the fact is the same: this is the one maven
 * document that genuinely mutates — a new version appears upstream with nothing here changing — so
 * it is cached with a TTL ({@code qits.artifacts.maven.proxy.metadata-ttl}) and revalidated with
 * {@link #etag}/{@link #lastModified}, unlike a jar, which is immutable and cached forever. When
 * upstream cannot be reached the stale document is served anyway: a build keeps resolving through a
 * Central outage, which is half of why the proxy exists.
 *
 * <p>{@link #doc} is upstream's document <b>verbatim</b>, and unlike a packument it needs no rewrite
 * at serve time: maven metadata carries versions, not URLs, so what upstream wrote is what a
 * resolver needs. That is also what makes the derived checksum honest — the bytes served and the
 * bytes hashed are the same bytes.
 *
 * <p>Two validators rather than one, because maven repositories are older than {@code ETag} is
 * universal: Central answers both, a mirror behind a plain file server may answer only {@code
 * Last-Modified}, and revalidating with whichever is present costs a {@code 304} instead of a
 * document.
 */
@Entity
@Table(name = "maven_proxy_metadata")
@IdClass(MavenProxyMetadataId.class)
public class MavenProxyMetadata extends PanacheEntityBase {

  @Id public String repository;

  /** The metadata path relative to the repository root, e.g. {@code org/slf4j/slf4j-api/maven-metadata.xml}. */
  @Id
  @Column(length = 1024)
  public String path;

  /**
   * <b>Not {@code @Lob}, and that is the whole of what makes this column work on PostgreSQL.</b>
   * Hibernate binds a {@code @Lob String} there as a large-object <em>locator</em> — a handle whose
   * bytes are only readable inside the transaction that produced it, so any read of the document
   * outside one dies with "Large Objects may not be used in auto-commit mode". {@code LONGVARCHAR}
   * binds the same column as ordinary text on PostgreSQL and on H2 alike, which is what a document
   * these routes read after their transaction has closed needs. Every consumer would otherwise have
   * to carry a dialect that rewrites the type; fixing it at the mapping is what means none does.
   */
  @JdbcTypeCode(SqlTypes.LONGVARCHAR)
  @Column(nullable = false)
  public String doc;

  /** Upstream's validator, replayed as {@code If-None-Match} once the TTL expires. */
  public String etag;

  /** The other validator, replayed as {@code If-Modified-Since} when there is no etag. */
  @Column(name = "last_modified")
  public String lastModified;

  @Column(name = "fetched_at", nullable = false)
  public Instant fetchedAt;
}
