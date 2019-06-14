/*
 * Copyright 2012-2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.join;

import org.springframework.data.annotation.Id;

/**
 * Book test class for N1QL Join tests
 */
public class Book {
    @Id
    String name;

    String authorName;

    String description;

    public Book(String name, String authorName, String description) {
        this.name = name;
        this.authorName = authorName;
        this.description = description;
    }

    public String getName() {
        return this.name;
    }

    public String getAuthorName() {
        return this.authorName;
    }

    public String getDescription() {
        return this.description;
    }
}