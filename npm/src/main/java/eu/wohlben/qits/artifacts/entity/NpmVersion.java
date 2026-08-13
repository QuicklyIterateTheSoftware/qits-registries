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
 * One version of one npm package, in one repository — published here, or pulled through the proxy.
 *
 * <p>The tarball's <em>bytes</em> are an ordinary {@code BlobStore} blob and {@link #tarballBlobId}
 * is its sha256 key, so npm tarballs dedupe globally with layers, screenshots and everything else.
 * npm's own two hashes live beside it rather than instead of it: {@link #shasum} is sha1 and {@link
 * #integrity} a base64 sha512 SRI string, neither addressable by the store, both verified end to
 * end by the client from the packument this row is re-emitted in.
 *
 * <p>{@link #manifestJson} is the version's manifest exactly as it arrived — every field npm cares
 * about, none of them modelled here, so a manifest gains a key without a schema change. Only {@code
 * dist} is replaced when the packument is assembled, because a tarball URL is a property of the
 * request's authority rather than of the package.
 *
 * <p>A row is immutable and there is no delete: re-publishing a version is {@code 403}. That is the
 * same append-only stance {@link OciManifest} takes, and the reason {@link NpmDistTag} exists.
 */
@Entity
@Table(name = "npm_version")
@IdClass(NpmVersionId.class)
public class NpmVersion extends PanacheEntityBase {

  @Id public String repository;

  @Id
  @Column(name = "package_name")
  public String packageName;

  @Id
  @Column(length = 128)
  public String version;

  @Column(name = "tarball_blob_id", nullable = false, length = 64)
  public String tarballBlobId;

  /** The base64 sha512 SRI string npm verifies the download against. Upstream's, for a proxy. */
  public String integrity;

  /** The legacy sha1 hex hash, still emitted because old clients still read it. */
  public String shasum;

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
  @Column(name = "manifest_json", nullable = false)
  public String manifestJson;

  @Column(name = "created_at", nullable = false)
  public Instant createdAt;

  /**
   * When a tarball GET last served this version, coalesced to once an hour; null means never read
   * since tracking began (V11). One column for both npm types — a proxied version is an ordinary row
   * here, so the pull that created it and every pull after it move the same timestamp. The proxy's
   * {@code npm_proxy_packument.fetched_at} is a different fact and stays what it is: that is when
   * the <em>document</em> was last revalidated upstream, not when these bytes were wanted.
   */
  @Column(name = "accessed_at")
  public Instant accessedAt;
}
