<?php
declare(strict_types=1);

/** Shared canonical-project lookup and merge support for Personal Tasks. */

function pp_normalise_project_name(string $name): string
{
    $name = preg_replace('/\s+/u', ' ', trim($name)) ?? trim($name);
    return mb_substr($name, 0, 255);
}

function pp_resolve_project(PDO $pdo, string $name): ?array
{
    $name = pp_normalise_project_name($name);
    if ($name === '') {
        return null;
    }

    $stmt = $pdo->prepare('
        SELECT p.id, p.name, p.category, p.status
        FROM personal_projects p
        WHERE p.archived_at IS NULL AND LOWER(p.name) = LOWER(:name)
        UNION ALL
        SELECT p.id, p.name, p.category, p.status
        FROM personal_project_aliases a
        JOIN personal_projects p ON p.id = a.canonical_project_id
        WHERE p.archived_at IS NULL AND LOWER(a.alias) = LOWER(:alias_name)
        LIMIT 1
    ');
    $stmt->execute([':name' => $name, ':alias_name' => $name]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    return is_array($row) ? $row : null;
}

function pp_find_or_create_project(PDO $pdo, string $name, ?string $description = null): ?array
{
    $name = pp_normalise_project_name($name);
    if ($name === '') {
        return null;
    }
    $project = pp_resolve_project($pdo, $name);
    if ($project !== null) {
        return $project;
    }

    $stmt = $pdo->prepare("INSERT INTO personal_projects (name, status, category, description) VALUES (:name, 'active', 'work', :description)");
    $stmt->execute([':name' => $name, ':description' => $description]);
    return [
        'id' => (int)$pdo->lastInsertId(),
        'name' => $name,
        'category' => 'work',
        'status' => 'active',
    ];
}

function pp_project_context(PDO $pdo): array
{
    $projects = $pdo->query('SELECT id, name FROM personal_projects WHERE archived_at IS NULL ORDER BY name ASC LIMIT 300')->fetchAll(PDO::FETCH_ASSOC) ?: [];
    $aliases = $pdo->query('
        SELECT a.alias, p.name AS canonical_name
        FROM personal_project_aliases a
        JOIN personal_projects p ON p.id = a.canonical_project_id
        WHERE p.archived_at IS NULL
        ORDER BY a.alias ASC
    ')->fetchAll(PDO::FETCH_ASSOC) ?: [];
    return ['projects' => $projects, 'aliases' => $aliases];
}

function pp_merge_project_into(PDO $pdo, int $canonicalProjectId, int $sourceProjectId): array
{
    if ($canonicalProjectId <= 0 || $sourceProjectId <= 0 || $canonicalProjectId === $sourceProjectId) {
        throw new InvalidArgumentException('Choose a different active project to merge.');
    }

    $pdo->beginTransaction();
    try {
        $projectStmt = $pdo->prepare('SELECT id, name, archived_at FROM personal_projects WHERE id IN (:canonical, :source) FOR UPDATE');
        $projectStmt->execute([':canonical' => $canonicalProjectId, ':source' => $sourceProjectId]);
        $projects = [];
        foreach ($projectStmt->fetchAll(PDO::FETCH_ASSOC) as $project) {
            $projects[(int)$project['id']] = $project;
        }
        $canonical = $projects[$canonicalProjectId] ?? null;
        $source = $projects[$sourceProjectId] ?? null;
        if (!$canonical || !$source || $canonical['archived_at'] !== null || $source['archived_at'] !== null) {
            throw new RuntimeException('Both projects must be active before they can be merged.');
        }

        $count = static function (string $table, string $column) use ($pdo, $sourceProjectId): int {
            $stmt = $pdo->prepare("SELECT COUNT(*) FROM {$table} WHERE {$column} = :source");
            $stmt->execute([':source' => $sourceProjectId]);
            return (int)$stmt->fetchColumn();
        };
        $counts = [
            'tasks' => $count('personal_tasks', 'project_id'),
            'notes' => $count('personal_notes', 'project_id'),
            'documents' => $count('personal_project_documents', 'project_id'),
            'task_links' => $count('personal_task_projects', 'project_id'),
            'tags' => $count('personal_project_tags', 'project_id'),
            'aliases' => $count('personal_project_aliases', 'canonical_project_id'),
        ];

        $move = static function (string $sql) use ($pdo, $canonicalProjectId, $sourceProjectId): void {
            $stmt = $pdo->prepare($sql);
            $params = [':source' => $sourceProjectId];
            if (str_contains($sql, ':canonical')) {
                $params[':canonical'] = $canonicalProjectId;
            }
            $stmt->execute($params);
        };
        $move('UPDATE personal_tasks SET project_id = :canonical WHERE project_id = :source');
        $move('UPDATE personal_notes SET project_id = :canonical WHERE project_id = :source');
        $move('INSERT INTO personal_project_documents (project_id, document_id, relevance_note) SELECT :canonical, src.document_id, src.relevance_note FROM personal_project_documents AS src WHERE src.project_id = :source ON DUPLICATE KEY UPDATE relevance_note = COALESCE(personal_project_documents.relevance_note, VALUES(relevance_note))');
        $move('DELETE FROM personal_project_documents WHERE project_id = :source');
        $move('INSERT INTO personal_task_projects (task_id, project_id, is_primary) SELECT src.task_id, :canonical, src.is_primary FROM personal_task_projects AS src WHERE src.project_id = :source ON DUPLICATE KEY UPDATE is_primary = GREATEST(personal_task_projects.is_primary, VALUES(is_primary))');
        $move('DELETE FROM personal_task_projects WHERE project_id = :source');
        $move('INSERT INTO personal_project_tags (project_id, tag_id) SELECT :canonical, src.tag_id FROM personal_project_tags AS src WHERE src.project_id = :source ON DUPLICATE KEY UPDATE project_id = VALUES(project_id)');
        $move('DELETE FROM personal_project_tags WHERE project_id = :source');
        $move('UPDATE personal_project_aliases SET canonical_project_id = :canonical WHERE canonical_project_id = :source');
        $aliasStmt = $pdo->prepare('INSERT INTO personal_project_aliases (canonical_project_id, alias) VALUES (:canonical, :alias) ON DUPLICATE KEY UPDATE canonical_project_id = VALUES(canonical_project_id)');
        $aliasStmt->execute([':canonical' => $canonicalProjectId, ':alias' => $source['name']]);
        $archiveStmt = $pdo->prepare("UPDATE personal_projects SET status = 'archived', archived_at = CURRENT_TIMESTAMP(6) WHERE id = :source");
        $archiveStmt->execute([':source' => $sourceProjectId]);

        $eventStmt = $pdo->prepare("INSERT INTO personal_events (event_type, target_entity_type, target_entity_id, payload_json, status, applied_at) VALUES ('merge_project', 'project', :target_id, :payload, 'applied', CURRENT_TIMESTAMP(6))");
        $eventStmt->execute([':target_id' => $canonicalProjectId, ':payload' => json_encode(['source_project_id' => $sourceProjectId, 'source_project_name' => $source['name'], 'counts' => $counts], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES)]);

        $pdo->commit();
        return ['canonical' => $canonical, 'source' => $source, 'counts' => $counts];
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
        throw $e;
    }
}
